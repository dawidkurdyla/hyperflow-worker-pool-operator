import os
import kopf
import kubernetes
import yaml
import logging
import sys
from copy import deepcopy
from os.path import exists
from helpers.helpers_queue import resolve_queue
from rabbitmq import RabbitAdmin

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


def rabbit_admin_from_env() -> RabbitAdmin:
    api = os.getenv("RABBIT_API")
    user = os.getenv("RABBIT_USER")
    pwd  = os.getenv("RABBIT_PASS")
    if not api or not user or not pwd:
        raise kopf.TemporaryError("RABBIT_API/RABBIT_USER/RABBIT_PASS not configured", delay=30)
    insecure = os.getenv("RABBIT_INSECURE", "0") == "1"
    
    return RabbitAdmin(api, user, pwd, verify=not insecure)

# Merge k8s EnvVar lists by name. Values from overrides win.
def _merge_env(base_env, override_env):
    base = [e for e in (base_env or []) if isinstance(e, dict) and e.get('name')]
    over = [e for e in (override_env or []) if isinstance(e, dict) and e.get('name')]
    by_name = {e['name']: e for e in base}
    for e in over:
        by_name[e['name']] = e

    order = [e['name'] for e in base]
    for e in over:
        if e['name'] not in order:
            order.append(e['name'])

    return [by_name[n] for n in order]


class WorkerPool:

    def __init__(self, name, namespace, spec):
        self.spec = spec
        self.name = name
        self.namespace = namespace

        self.min_replica_count = spec['minReplicaCount']
        self.image = spec['image']
        self.rabbitHostname = spec['rabbitHostname']
        self.redisUrl = spec['redisUrl']
        self.prometheusUrl = spec["prometheusUrl"]
        self.cpu_requests = spec['initialResources']['requests']['cpu']
        self.memory_requests = spec['initialResources']['requests']['memory']

        q = resolve_queue(spec)
        self.queue_name = q['name']
        self.queue_vhost = q['vhost']
        self.queue_durable = q['durable']
        self.queue_auto_delete = q['autoDelete']
        self.queue_arguments = q['arguments']

        # optional
        self.max_replica_count = None
        self.cpu_limits = None
        self.memory_limits = None
        self.env = spec.get('env', []) or []

        if 'maxReplicaCount' in spec:
            self.max_replica_count = spec['maxReplicaCount']
        if 'limits' in spec['initialResources'] and 'cpu' in spec['initialResources']['limits']:
            self.cpu_limits = spec['initialResources']['limits']['cpu']
        if 'limits' in spec['initialResources'] and 'memory' in spec['initialResources']['limits']:
            self.memory_limits = spec['initialResources']['limits']['memory']


@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    settings.posting.level = logging.INFO
    if not exists("/templates/deployment.yml"):
        raise Exception("Initialization error: /templates/deployment.yml file does not exist")
    if not exists("/templates/prometheus-rule.yml"):
        raise Exception("Initialization error: /templates/prometheus-rule.yml file does not exist")
    if not exists("/templates/scaledobject.yml"):
        raise Exception("Initialization error: /templates/scaledobject.yml file does not exist")



@kopf.on.create('hyperflow.agh.edu.pl', 'v1', 'workerpools')
def create_fn(body, spec, patch, **kwargs):
    k8s_client = kubernetes.client
    name = body['metadata']['name']
    namespace = body['metadata']['namespace']

    status = {"status": {"workerPoolName": name, "conditions": [get_initializing_condition()]}}
    update_worker_pool_status(k8s_client, name, namespace, status)
    try:
        worker_pool = WorkerPool(name, namespace, spec)

        create_worker_pool_queue(worker_pool)
        create_worker_pool_deployment(k8s_client, body, worker_pool)
        create_worker_pool_prometheusrule(k8s_client, body, worker_pool)
        create_worker_pool_scaledobject(k8s_client, body, worker_pool)
        status['status']['conditions'] = [get_ready_condition()] + status['status']['conditions']
    except Exception as e:
        logging.exception(f"Worker pool {name} initialization failed.")
        status['status']['conditions'] = [get_error_condition(str(e))] + status['status']['conditions']
    finally:
        update_worker_pool_status(k8s_client, name, namespace, status)


@kopf.on.update('hyperflow.agh.edu.pl', 'v1', 'workerpools')
def update_fn(spec, status, namespace, old, new, **kwargs):
    logging.info(f"Patch spec: {spec}")
    logging.info(f"Patch status: {status}")

    k8s_client = kubernetes.client
    name = status['workerPoolName']

    status_patch = {"status": {"conditions": [get_updating_condition()]}}
    update_worker_pool_status(k8s_client, name, namespace, status_patch)
    try:
        worker_pool = WorkerPool(name, namespace, spec)
        old_spec = (old or {}).get('spec', {})

        patch_worker_pool_queue(old_spec, worker_pool)
        patch_worker_pool_deployment(k8s_client, worker_pool)
        patch_worker_pool_prometheusrule(k8s_client, worker_pool)
        patch_worker_pool_scaledobject(k8s_client, worker_pool)

        status_patch['status']['conditions'] = [get_ready_condition()] + status_patch['status']['conditions']
    except Exception as e:
        logging.exception(f"Worker pool {name} updating failed.")
        status_patch['status']['conditions'] = [get_error_condition(str(e))] + status_patch['status']['conditions']
    finally:
        update_worker_pool_status(k8s_client, name, namespace, status_patch)


@kopf.on.delete('hyperflow.agh.edu.pl', 'v1', 'workerpools')
def delete(body, spec, **kwargs):
    name = body['metadata']['name']
    namespace = body['metadata']['namespace']
    logging.info(f"Deleting worker pool {name} and its children")

    worker_pool = WorkerPool(name, namespace, spec)
    delete_worker_pool_queue(worker_pool)


def create_worker_pool_deployment(k8s_client, body, worker_pool):
    deployment = parse_deployment_template(worker_pool)
    kopf.adopt(deployment, owner=body)
    apps_api = k8s_client.AppsV1Api()
    obj = apps_api.create_namespaced_deployment(worker_pool.namespace, deployment)
    logging.info(f"Deployment {worker_pool.name} created")


def patch_worker_pool_deployment(k8s_client, worker_pool):
    deployment = parse_deployment_template(worker_pool)
    apps_api = k8s_client.AppsV1Api()
    obj = apps_api.patch_namespaced_deployment(worker_pool.name, worker_pool.namespace, {"spec": deployment['spec']})
    logging.info(f"Deployment {worker_pool.name} patched")


def create_worker_pool_prometheusrule(k8s_client, body, worker_pool):
    prometheus_rule = parse_prometheusrule_template(worker_pool)
    kopf.adopt(prometheus_rule, owner=body)
    crd_api = k8s_client.CustomObjectsApi()
    obj = crd_api.create_namespaced_custom_object(group="monitoring.coreos.com",
                                                  version="v1",
                                                  namespace=worker_pool.namespace,
                                                  plural="prometheusrules",
                                                  body=prometheus_rule)
    logging.info(f"PrometheusRule {worker_pool.name} created")


def patch_worker_pool_prometheusrule(k8s_client, worker_pool):
    prometheus_rule = parse_prometheusrule_template(worker_pool)
    crd_api = k8s_client.CustomObjectsApi()
    obj = crd_api.patch_namespaced_custom_object(name=worker_pool.name,
                                                 group="monitoring.coreos.com",
                                                 version="v1",
                                                 namespace=worker_pool.namespace,
                                                 plural="prometheusrules",
                                                 body={"spec": prometheus_rule['spec']})
    logging.info(f"PrometheusRule {worker_pool.name} patched")


def create_worker_pool_scaledobject(k8s_client, body, worker_pool):
    scaledobject = parse_scaledobject_template(worker_pool)
    kopf.adopt(scaledobject, owner=body)
    crd_api = k8s_client.CustomObjectsApi()
    obj = crd_api.create_namespaced_custom_object(group="keda.sh",
                                                  version="v1alpha1",
                                                  namespace=worker_pool.namespace,
                                                  plural="scaledobjects",
                                                  body=scaledobject)
    logging.info(f"ScaledObject {worker_pool.name} created")


def patch_worker_pool_scaledobject(k8s_client, worker_pool):
    scaledobject = parse_scaledobject_template(worker_pool)
    crd_api = k8s_client.CustomObjectsApi()
    obj = crd_api.patch_namespaced_custom_object(name=worker_pool.name,
                                                 group="keda.sh",
                                                 version="v1alpha1",
                                                 namespace=worker_pool.namespace,
                                                 plural="scaledobjects",
                                                 body={"spec": scaledobject['spec']})
    logging.info(f"ScaledObject {worker_pool.name} patched")


def create_worker_pool_queue(worker_pool):
    admin = rabbit_admin_from_env()
    admin.ensure_queue(worker_pool.queue_vhost,
                        worker_pool.queue_name,
                        worker_pool.queue_durable,
                        worker_pool.queue_auto_delete,
                        worker_pool.queue_arguments)


def patch_worker_pool_queue(old_spec, worker_pool):
    admin = rabbit_admin_from_env()
    old_q = resolve_queue(old_spec) if old_spec else None
    new_q = {
        "name": worker_pool.queue_name,
        "vhost": worker_pool.queue_vhost,
        "durable": worker_pool.queue_durable,
        "autoDelete": worker_pool.queue_auto_delete,
        "arguments": worker_pool.queue_arguments,
    }
    changed = (not old_q) or any(new_q[k] != old_q.get(k) for k in ("name", "vhost", "durable", "autoDelete", "arguments"))

    if changed and old_q:
        try:
            admin.delete_queue(old_q["vhost"], old_q["name"])
        except Exception as e:
            logging.warning(f"Delete old queue failed: {e}")

        admin.ensure_queue(new_q["vhost"], new_q["name"], new_q["durable"], new_q["autoDelete"], new_q["arguments"])


def delete_worker_pool_queue(worker_pool):
    admin = rabbit_admin_from_env()

    try:
        admin.delete_queue(worker_pool.queue_vhost, worker_pool.queue_name)
    except Exception as e:
        # Raising TemporaryError will keep the finalizer until RabbitMQ is reachable again
        raise kopf.TemporaryError(f"Queue delete error: {e}", delay=30)


def parse_deployment_template(worker_pool):
    with open(r'/templates/deployment.yml') as file:
        template = file.read().format(poolName=worker_pool.name,
                                      namespace=worker_pool.namespace,
                                      image=worker_pool.image,
                                      rabbitHostname=worker_pool.rabbitHostname,
                                      redisUrl=worker_pool.redisUrl,
                                      queueName=worker_pool.queue_name,
                                      cpuRequests=worker_pool.cpu_requests,
                                      memoryRequests=worker_pool.memory_requests,
                                      minReplicas=worker_pool.min_replica_count)
        deployment = yaml.safe_load(template)

        try:
            container = deployment['spec']['template']['spec']['containers'][0]
            base_env = container.get('env', [])
            merged = _merge_env(base_env, worker_pool.env)
            container['env'] = merged
        except Exception as e:
            logging.warning(f"env merge skipped: {e}")

        limits = {}
        if worker_pool.cpu_limits is not None:
            limits['cpu'] = worker_pool.cpu_limits
        if worker_pool.memory_limits is not None:
            limits['memory'] = worker_pool.memory_limits
        if len(limits.keys()) > 0:
            deployment['spec']['template']['spec']['containers'][0]['resources']['limits'] = limits

        return deployment


def parse_prometheusrule_template(worker_pool):
    with open(r'/templates/prometheus-rule.yml') as file:
        template = file.read().format(poolName=worker_pool.name,
                                      namespace=worker_pool.namespace,
                                      queueName=worker_pool.queue_name,
                                      poolNameUnderscored=worker_pool.name.replace('-', '_'),
                                      cpuLimits=worker_pool.cpu_requests,
                                      memoryLimits=worker_pool.memory_requests)
        return yaml.safe_load(template)


def parse_scaledobject_template(worker_pool):
    with open(r'/templates/scaledobject.yml') as file:
        template = file.read().format(poolName=worker_pool.name,
                                      namespace=worker_pool.namespace,
                                      queueName=worker_pool.queue_name,
                                      prometheusUrl=worker_pool.prometheusUrl,
                                      poolNameUnderscored=worker_pool.name.replace('-', '_'),
                                      minReplicaCount=worker_pool.min_replica_count)
        scaledobject = yaml.safe_load(template)
        if worker_pool.max_replica_count is not None:
            scaledobject['spec']['maxReplicaCount'] = worker_pool.max_replica_count
        return scaledobject


def update_worker_pool_status(k8s_client, name, namespace, status):
    crd_api = k8s_client.CustomObjectsApi()
    crd_api.patch_namespaced_custom_object(name=name,
                                           group="hyperflow.agh.edu.pl",
                                           version="v1",
                                           namespace=namespace,
                                           plural="workerpools",
                                           body=status)


def get_initializing_condition():
    condition = {'message': "WorkerPool is being initialized",
                 'reason': "WorkerPoolInitializing",
                 "status": "False",
                 "type": "NotReady"}
    return condition


def get_updating_condition():
    condition = {'message': "WorkerPool is being updated",
                 'reason': "WorkerPoolUpdating",
                 "status": "False",
                 "type": "NotReady"}
    return condition


def get_ready_condition():
    condition = {'message': "Worker pool is ready for processing workflows",
                 'reason': "WorkerPoolReady",
                 "status": "True",
                 "type": "Ready"}
    return condition


def get_error_condition(error_message):
    condition = {'message': f"Worker pool initialization error: {error_message}",
                 'reason': "WorkerPoolError",
                 "status": "False",
                 "type": "NotReady"}
    return condition
