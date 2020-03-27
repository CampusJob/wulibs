
import kubernetes
import logging


class Kubernetes:

    def __init__(self):
        kubernetes.config.load_incluster_config()
        self.k8s = kubernetes.client.CoreV1Api()

    def get_service(self, namespace, service_name):
        try:
            for service in self.k8s.list_namespaced_service(namespace=namespace).items:
                if service.metadata.name == service_name:
                    return service

        except Exception as e:
            logging.error(f"unable to get service [{service_name}] in namespace [{namespace}]: {e}")
            raise e

    def update_service(self, domain):
        service_definition = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": domain.service_name,
                "namespace": domain.namespace,
            },
            "spec": {
                "ports": [
                    {
                        "port": domain.port,
                        "targetPort": domain.target_port,
                    },
                ]
            }
        }

        service = self.get_service(domain.namespace, domain.service_name)
        if service:
            logging.info(f"updating service [{domain.service_name}]")
            try:
                self.k8s.patch_namespaced_service(
                    name=domain.service_name, namespace=domain.namespace, body=service_definition)

            except Exception as e:
                logging.error(f"unable to update service [{domain.service_name}] "
                              f"in namespace [{domain.namespace}]: {e}")
                raise e
        else:
            logging.info(f"creating service [{domain.service_name}] in namespace [{domain.namespace}]")
            try:
                self.k8s.create_namespaced_service(
                    namespace=domain.namespace, body=service_definition)

            except Exception as e:
                logging.error(f"unable to create service [{domain.service_name}] "
                              f"in namespace [{domain.namespace}]: {e}")
                raise e

    def get_service_endpoints(self, namespace, service_name):
        try:
            for endpoint in self.k8s.list_namespaced_endpoints(namespace=namespace).items:
                if endpoint.metadata.name == service_name:
                    return endpoint

        except Exception as e:
            logging.error(f"unable to get endpoints for service [{service_name}] in namespace [{namespace}]: {e}")
            raise e

    def update_endpoints(self, domain, addresses):
        endpoints_definition = {
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {
                "name": domain.service_name,
                "namespace": domain.namespace,
            },
            "subsets": [
                {
                    "addresses": [{"ip": address} for address in addresses],
                    "ports": [{"port": domain.target_port}]
                }
            ]
        }

        service_endpoint = self.get_service_endpoints(domain.namespace, domain.service_name)
        if service_endpoint:
            logging.info(f"updating endpoints [{domain.service_name}] in namespace [{domain.namespace}] "
                         f"with addresses [{', '.join(addresses)}]")
            try:
                self.k8s.patch_namespaced_endpoints(
                    name=domain.service_name, namespace=domain.namespace, body=endpoints_definition)

            except Exception as e:
                logging.error(f"unable to update endpoints [{domain.service_name}] "
                              f"in namespace [{domain.namespace}]: {e}")
                raise e

        else:
            logging.info(f"creating endpoints [{domain.service_name}] in namespace [{domain.namespace}] "
                         f"with addresses [{', '.join(addresses)}]")
            try:
                self.k8s.create_namespaced_endpoints(
                    namespace=domain.namespace, body=endpoints_definition)

            except Exception as e:
                logging.error(f"unable to create endpoints [{domain.service_name}] "
                              f"in namespace [{domain.namespace}]: {e}")
                raise e
