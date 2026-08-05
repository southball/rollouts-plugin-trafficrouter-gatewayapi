package plugin

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/utils/weightutil"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayApiClientset "sigs.k8s.io/gateway-api/pkg/client/clientset/versioned"
)

const experimentServicesAnnotation = "rollouts.argoproj.io/gatewayapi-experiment-services"

func HandleExperiment(ctx context.Context, clientset *kubernetes.Clientset, gatewayClient gatewayApiClientset.Interface, logger *logrus.Entry, rollout *v1alpha1.Rollout, httpRoute *gatewayv1.HTTPRoute, additionalDestinations []v1alpha1.WeightDestination) error {
	ruleIdx := -1
	stableService := rollout.Spec.Strategy.Canary.StableService
	canaryService := rollout.Spec.Strategy.Canary.CanaryService

	for i, rule := range httpRoute.Spec.Rules {
		if ruleIdx != -1 {
			break
		}
		for _, backendRef := range rule.BackendRefs {
			if string(backendRef.Name) == stableService || string(backendRef.Name) == canaryService {
				ruleIdx = i
				break
			}
		}
	}

	if ruleIdx == -1 {
		return fmt.Errorf("no matching rule found for rollout %s", rollout.Name)
	}

	isExperimentActive := rollout.Spec.Strategy.Canary != nil && rollout.Status.Canary.CurrentExperiment != ""

	// Keep ownership on the route because Argo Rollouts can clear the additional
	// destinations from status before it clears CurrentExperiment. Status remains
	// a fallback for routes created by plugin versions that predate the annotation.
	experimentServices := experimentServicesFromHTTPRoute(httpRoute)
	if rollout.Status.Canary.Weights != nil {
		for _, dest := range rollout.Status.Canary.Weights.Additional {
			if dest.ServiceName != "" {
				experimentServices[dest.ServiceName] = true
			}
		}
	}

	if isExperimentActive {
		logger.Info(fmt.Sprintf("Found active experiment %s", rollout.Status.Canary.CurrentExperiment))

		if len(additionalDestinations) == 0 {
			logger.Info("No experiment services found in additionalDestinations, skipping experiment service addition")
			return nil
		}

		for _, dest := range additionalDestinations {
			if dest.ServiceName != "" {
				experimentServices[dest.ServiceName] = true
			}
		}
		setExperimentServicesAnnotation(httpRoute, experimentServices)

		// Preserve the canary allocation established by SetWeight. Experiment
		// destinations consume traffic in addition to canary, so only the
		// remainder belongs to stable.
		var canaryWeight int32
		for _, backendRef := range httpRoute.Spec.Rules[ruleIdx].BackendRefs {
			if string(backendRef.Name) == canaryService && backendRef.Weight != nil {
				canaryWeight = *backendRef.Weight
				break
			}
		}

		var totalExperimentWeight int64
		for _, dest := range additionalDestinations {
			totalExperimentWeight += int64(dest.Weight)
		}

		maxWeight := weightutil.MaxTrafficWeight(rollout)
		totalAllocatedWeight := int64(canaryWeight) + totalExperimentWeight
		stableWeight := int32(0)
		if totalAllocatedWeight < int64(maxWeight) {
			stableWeight = maxWeight - int32(totalAllocatedWeight)
		} else if totalAllocatedWeight > int64(maxWeight) {
			logger.Warnf("Combined canary and experiment weight exceeds maxTrafficWeight %d (got %d), setting stable weight to 0", maxWeight, totalAllocatedWeight)
		}

		for i, backendRef := range httpRoute.Spec.Rules[ruleIdx].BackendRefs {
			if string(backendRef.Name) == stableService {
				httpRoute.Spec.Rules[ruleIdx].BackendRefs[i].Weight = &stableWeight
				break
			}
		}

		for _, additionalDest := range additionalDestinations {
			serviceName := additionalDest.ServiceName
			weight := additionalDest.Weight

			exists := false
			for _, backendRef := range httpRoute.Spec.Rules[ruleIdx].BackendRefs {
				if string(backendRef.Name) == serviceName {
					exists = true
					break
				}
			}

			if !exists {
				logger.Info(fmt.Sprintf("Adding experiment service to HTTPRoute: %s with weight %d", serviceName, weight))

				service, err := clientset.CoreV1().Services(rollout.Namespace).Get(ctx, serviceName, metav1.GetOptions{})
				if err != nil {
					logger.Warn(fmt.Sprintf("Failed to get service %s: %v", serviceName, err))
					continue
				}

				port := gatewayv1.PortNumber(8080)
				portName := "http"
				for _, servicePort := range service.Spec.Ports {
					if servicePort.Name == portName {
						port = servicePort.Port
						break
					}
				}

				if len(service.Spec.Ports) > 0 && port == 8080 {
					port = service.Spec.Ports[0].Port
				}

				namespace := gatewayv1.Namespace(rollout.Namespace)
				httpRoute.Spec.Rules[ruleIdx].BackendRefs = append(httpRoute.Spec.Rules[ruleIdx].BackendRefs, gatewayv1.HTTPBackendRef{
					BackendRef: gatewayv1.BackendRef{
						BackendObjectReference: gatewayv1.BackendObjectReference{
							Name:      gatewayv1.ObjectName(serviceName),
							Namespace: &namespace,
							Port:      &port,
						},
						Weight: &weight,
					},
				})
			}
		}
		return nil
	}

	if !isExperimentActive && len(experimentServices) > 0 {
		logger.Info("Experiment is no longer active, removing experiment services from HTTPRoute")

		stableWeight := weightutil.MaxTrafficWeight(rollout)
		filteredBackendRefs := []gatewayv1.HTTPBackendRef{}

		for _, backendRef := range httpRoute.Spec.Rules[ruleIdx].BackendRefs {
			serviceName := string(backendRef.Name)

			if experimentServices[serviceName] {
				logger.Info(fmt.Sprintf("Removing experiment service from HTTPRoute: %s", serviceName))
				continue
			}

			switch serviceName {
			case stableService:
				backendRef.Weight = &stableWeight
			case canaryService:
				zeroWeight := int32(0)
				backendRef.Weight = &zeroWeight
			}
			filteredBackendRefs = append(filteredBackendRefs, backendRef)
		}

		httpRoute.Spec.Rules[ruleIdx].BackendRefs = filteredBackendRefs
		setExperimentServicesAnnotation(httpRoute, nil)
		logger.Info("Experiment services removed from HTTPRoute")
	}

	return nil
}

func experimentServicesFromHTTPRoute(httpRoute *gatewayv1.HTTPRoute) map[string]bool {
	services := make(map[string]bool)
	for serviceName := range strings.SplitSeq(httpRoute.Annotations[experimentServicesAnnotation], ",") {
		if serviceName != "" {
			services[serviceName] = true
		}
	}
	return services
}

func setExperimentServicesAnnotation(httpRoute *gatewayv1.HTTPRoute, services map[string]bool) {
	annotations := httpRoute.GetAnnotations()
	if len(services) == 0 {
		delete(annotations, experimentServicesAnnotation)
		if len(annotations) == 0 {
			httpRoute.SetAnnotations(nil)
		} else {
			httpRoute.SetAnnotations(annotations)
		}
		return
	}

	serviceNames := make([]string, 0, len(services))
	for serviceName := range services {
		serviceNames = append(serviceNames, serviceName)
	}
	sort.Strings(serviceNames)
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[experimentServicesAnnotation] = strings.Join(serviceNames, ",")
	httpRoute.SetAnnotations(annotations)
}
