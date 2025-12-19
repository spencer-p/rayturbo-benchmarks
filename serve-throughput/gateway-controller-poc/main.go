package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayclientset "sigs.k8s.io/gateway-api/pkg/client/clientset/versioned"
)

type AppsResponse struct {
	TargetGroups []TargetGroups `json:"target_groups"`
}

type TargetGroups struct {
	Targets     []Target `json:"targets"`
	RoutePrefix string   `json:"route_prefix"`
	Protocol    string   `json:"protocol"`
	AppName     string   `json:"app_name"`
}

type Target struct {
	IP         string `json:"ip"`
	Port       int    `json:"port"`
	InstanceID string `json:"instance_id"`
	Name       string `json:"name"`
}

func main() {
	filter := flag.String("filter", "throughput", "Cluster name to filter to (Ray Cluster must include this string to reconcile)")
	flag.Parse()
	fmt.Println(*filter)

	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	gatewayClient, err := gatewayclientset.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	// TODO: Use RayService objects as the start of discovery, not ray cluster
	// head services.
	labelSelector := "ray.io/node-type=head,ray.io/cluster"
	listOptions := metav1.ListOptions{
		LabelSelector: labelSelector,
	}

	services, err := clientset.CoreV1().Services("").List(context.TODO(), listOptions)
	if err != nil {
		panic(err.Error())
	}

	fmt.Printf("Services with label '%s':\n", labelSelector)
	for _, service := range services.Items {
		fmt.Printf("Visit service %s\n", service.GetName())
		if !strings.Contains(service.GetName(), *filter) {
			continue
		}
		if err := reconcileService(clientset, gatewayClient, dynamicClient, service); err != nil {
			fmt.Println("failed to reconcile:", err)
		}
	}
}

func fetchTargets(kc *kubernetes.Clientset, svc corev1.Service) ([]TargetGroups, error) {
	rawpath := fmt.Sprintf("/api/v1/namespaces/%s/services/%s:8265/proxy/api/serve/applications/", svc.GetNamespace(), svc.GetName())
	raw, err := kc.RESTClient().Get().AbsPath(rawpath).DoRaw(context.TODO())
	if err != nil {
		return nil, err
	}
	var resp AppsResponse
	err = json.Unmarshal(raw, &resp)
	return resp.TargetGroups, err
}

func reconcileService(kc *kubernetes.Clientset, gc *gatewayclientset.Clientset, dc *dynamic.DynamicClient, svc corev1.Service) error {
	clusterName := svc.GetLabels()["ray.io/cluster"]
	if clusterName == "" {
		fmt.Println("skip", svc.GetName())
		return nil
	}
	fmt.Println(clusterName)

	tgs, err := fetchTargets(kc, svc)
	if err != nil {
		return fmt.Errorf("fetch target groups: %w", err)
	}
	if len(tgs) == 0 {
		fmt.Println("no target groups; no ray serve")
		return nil
	}

	pods, err := kc.CoreV1().Pods(svc.GetNamespace()).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list pods: %w", err)
	}
	ipToPod := make(map[string]corev1.Pod)
	for _, pod := range pods.Items {
		ipToPod[pod.Status.PodIP] = pod
	}

	for _, tg := range tgs {
		fmt.Printf("- %s\n", tg.AppName)
		endpointSlices := generateEndpointSlices(clusterName, svc.GetNamespace(), tg.AppName, tg.Targets, ipToPod)
		services := generateServices(clusterName, svc.GetNamespace(), tg.AppName, endpointSlices)
		for _, s := range services {
			data, err := json.Marshal(s)
			if err != nil {
				fmt.Printf("failed to marshal service %s: %v\n", s.Name, err)
				continue
			}
			_, err = kc.CoreV1().Services(s.Namespace).Patch(context.TODO(), s.Name, types.ApplyPatchType, data, metav1.PatchOptions{FieldManager: "rayserve-gateway"})
			if err != nil {
				fmt.Printf("failed to apply service %s: %v\n", s.Name, err)
			} else {
				fmt.Printf("Applied Service %s\n", s.Name)
			}

			hcp := generateHealthCheckPolicy(s.Name, s.Namespace)
			hcpData, err := json.Marshal(hcp)
			if err != nil {
				fmt.Printf("failed to marshal healthcheckpolicy %s: %v\n", s.Name, err)
				continue
			}
			hcpRes := schema.GroupVersionResource{Group: "networking.gke.io", Version: "v1", Resource: "healthcheckpolicies"}
			_, err = dc.Resource(hcpRes).Namespace(s.Namespace).Patch(context.TODO(), s.Name, types.ApplyPatchType, hcpData, metav1.PatchOptions{FieldManager: "rayserve-gateway"})
			if err != nil {
				fmt.Printf("failed to apply healthcheckpolicy %s: %v\n", s.Name, err)
			} else {
				fmt.Printf("Applied HealthCheckPolicy %s\n", s.Name)
			}
		}

		for _, es := range endpointSlices {
			data, err := json.Marshal(es)
			if err != nil {
				fmt.Printf("failed to marshal endpointslice %s: %v\n", es.Name, err)
				continue
			}
			_, err = kc.DiscoveryV1().EndpointSlices(es.Namespace).Patch(context.TODO(), es.Name, types.ApplyPatchType, data, metav1.PatchOptions{FieldManager: "rayserve-gateway"})
			if err != nil {
				fmt.Printf("failed to apply endpointslice %s: %v\n", es.Name, err)
			} else {
				fmt.Printf("Applied EndpointSlice %s with %d targets\n", es.Name, len(es.Endpoints))
			}
		}

		// Generate the HTTP route encompassing the services and apply.
		hr := generateHTTPRoutes(clusterName, svc.GetNamespace(), tg.AppName, tg.RoutePrefix, endpointSlices)
		data, err := json.Marshal(hr)
		if err != nil {
			fmt.Printf("failed to marshal httproute %s: %v\n", hr.Name, err)
			continue
		}
		var hrMap map[string]any
		if err := json.Unmarshal(data, &hrMap); err != nil {
			fmt.Printf("failed to unmarshal httproute to map %s: %v\n", hr.Name, err)
			continue
		}
		delete(hrMap, "status")
		data, err = json.Marshal(hrMap)
		if err != nil {
			fmt.Printf("failed to marshal httproute map %s: %v\n", hr.Name, err)
			continue
		}
		_, err = gc.GatewayV1().HTTPRoutes(hr.Namespace).Patch(context.TODO(), hr.Name, types.ApplyPatchType, data, metav1.PatchOptions{FieldManager: "rayserve-gateway"})
		if err != nil {
			fmt.Printf("failed to apply httproute %s: %v\n", hr.Name, err)
		} else {
			fmt.Printf("Applied HTTPRoute %s\n", hr.Name)
		}
	}

	gateway := generateGateway(clusterName, svc.GetNamespace())
	data, err := json.Marshal(gateway)
	if err != nil {
		return fmt.Errorf("failed to marshal gateway %s: %w", gateway.Name, err)
	}
	_, err = gc.GatewayV1().Gateways(gateway.Namespace).Patch(context.TODO(), gateway.Name, types.ApplyPatchType, data, metav1.PatchOptions{FieldManager: "rayserve-gateway"})
	if err != nil {
		fmt.Printf("failed to apply gateway %s: %v\n", gateway.Name, err)
	} else {
		fmt.Printf("Applied Gateway %s\n", gateway.Name)
	}

	return nil
}

func generateServices(name, namespace, appname string, endpointSlices []discoveryv1.EndpointSlice) []corev1.Service {
	svcs := []corev1.Service{}
	for _, es := range endpointSlices {
		port := *es.Ports[0].Port
		svcs = append(svcs, corev1.Service{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "v1",
				Kind:       "Service",
			},
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      es.GetLabels()["kubernetes.io/service-name"],
				Labels: map[string]string{
					"kubernetes.io/created-by": "rayserve-gateway",
				},
			},
			Spec: corev1.ServiceSpec{
				Ports: []corev1.ServicePort{{
					Port:       80,
					TargetPort: intstr.FromInt32(port),
				}},
				PublishNotReadyAddresses: true,
			},
		})
	}
	return svcs
}

func generateEndpointSlices(name, namespace, appname string, targets []Target, ipToPod map[string]corev1.Pod) []discoveryv1.EndpointSlice {
	byPort := make(map[int32]discoveryv1.EndpointSlice)
	for _, target := range targets {
		port := int32(target.Port)
		existing, ok := byPort[port]
		sliceName := sanitize(fmt.Sprintf("%s-%s-%d", name, appname, port))
		if !ok {
			existing = discoveryv1.EndpointSlice{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "discovery.k8s.io/v1",
					Kind:       "EndpointSlice",
				},
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      sliceName,
					Labels: map[string]string{
						"kubernetes.io/service-name": sliceName,
						"kubernetes.io/created-by":   "rayserve-gateway",
					},
				},
				AddressType: discoveryv1.AddressTypeIPv4,
				Ports: []discoveryv1.EndpointPort{{
					Port: &port,
				}},
			}
		}
		endpoint := discoveryv1.Endpoint{
			Addresses: []string{target.IP},
			Conditions: discoveryv1.EndpointConditions{
				Ready:   ptr(true),
				Serving: ptr(true),
			},
		}
		if pod, ok := ipToPod[target.IP]; ok {
			endpoint.NodeName = ptr(pod.Spec.NodeName)
			endpoint.TargetRef = &corev1.ObjectReference{
				Kind:      "Pod",
				Namespace: pod.Namespace,
				Name:      pod.Name,
				UID:       pod.UID,
			}
		}
		existing.Endpoints = append(existing.Endpoints, endpoint)
		byPort[port] = existing
	}
	return slices.Collect(maps.Values(byPort))
}

func generateGateway(name, namespace string) gatewayv1.Gateway {
	return gatewayv1.Gateway{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "gateway.networking.k8s.io/v1",
			Kind:       "Gateway",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels: map[string]string{
				"kubernetes.io/created-by": "rayserve-gateway",
			},
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "gke-l7-rilb",
			Listeners: []gatewayv1.Listener{{
				Name:     "http",
				Protocol: gatewayv1.HTTPProtocolType,
				Port:     80,
				AllowedRoutes: &gatewayv1.AllowedRoutes{
					Namespaces: &gatewayv1.RouteNamespaces{
						From: ptr(gatewayv1.NamespacesFromSame),
					},
				},
			}},
		},
	}
}

func generateHTTPRoutes(name, namespace, appname, routePrefix string, endpointSlices []discoveryv1.EndpointSlice) gatewayv1.HTTPRoute {
	rules := []gatewayv1.HTTPRouteRule{{
		Matches: []gatewayv1.HTTPRouteMatch{{
			Path: &gatewayv1.HTTPPathMatch{
				Type:  ptr(gatewayv1.PathMatchPathPrefix),
				Value: ptr(routePrefix),
			},
		}},
	}}

	for _, es := range endpointSlices {
		serviceName := es.Labels["kubernetes.io/service-name"]
		weight := int32(len(es.Endpoints))
		rules[0].BackendRefs = append(rules[0].BackendRefs, gatewayv1.HTTPBackendRef{
			BackendRef: gatewayv1.BackendRef{
				BackendObjectReference: gatewayv1.BackendObjectReference{
					Name: gatewayv1.ObjectName(serviceName),
					Port: ptr(gatewayv1.PortNumber(80)),
				},
				Weight: ptr(weight),
			},
		})
	}

	return gatewayv1.HTTPRoute{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "gateway.networking.k8s.io/v1",
			Kind:       "HTTPRoute",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      sanitize(fmt.Sprintf("%s-%s", name, appname)),
			Labels: map[string]string{
				"kubernetes.io/created-by": "rayserve-gateway",
			},
		},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{{
					Name: gatewayv1.ObjectName(name),
				}},
			},
			Rules: rules,
		},
	}
}

func ptr[T any](v T) *T {
	return &v
}

func sanitize(s string) string {
	return strings.ReplaceAll(s, "_", "-")
}

func generateHealthCheckPolicy(name, namespace string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "networking.gke.io/v1",
			"kind":       "HealthCheckPolicy",
			"metadata": map[string]any{
				"name":      name,
				"namespace": namespace,
				"labels": map[string]string{
					"kubernetes.io/created-by": "rayserve-gateway",
				},
			},
			"spec": map[string]any{
				"default": map[string]any{
					"config": map[string]any{
						"type": "HTTP",
						"httpHealthCheck": map[string]any{
							"requestPath": "/-/healthz",
						},
					},
				},
				"targetRef": map[string]any{
					"group": "",
					"kind":  "Service",
					"name":  name,
				},
			},
		},
	}
}
