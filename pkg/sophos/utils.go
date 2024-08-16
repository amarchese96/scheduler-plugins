package sophos

import (
	"context"
	"math"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func AreLesserOrderPodsScheduled(ctx context.Context, handle framework.Handle, pod *v1.Pod) bool {
	namespace := pod.GetNamespace()

	appGroup, ok := pod.GetLabels()["app-group"]
	if !ok {
		klog.Infof("error getting app-group label for pod %s", pod.Name)
		return false
	}

	for key, value := range pod.GetLabels() {
		if strings.HasPrefix(key, "chain-") {
			index, err := strconv.Atoi(value)
			if err != nil {
				klog.Infof("error parsing chain label value for pod %s", pod.Name)
				return false
			}

			if index > 0 {
				labelSelector := metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app-group": appGroup,
						key:         strconv.Itoa(index - 1),
					},
				}
				listOptions := metav1.ListOptions{
					LabelSelector: labels.Set(labelSelector.MatchLabels).String(),
				}
				lesserOrderPods, err := handle.ClientSet().CoreV1().Pods(namespace).List(ctx, listOptions)
				if err != nil {
					klog.Infof("error getting lesser order pods for pod %s", pod.Name)
					return false
				}

				if len(lesserOrderPods.Items) == 0 {
					return false
				}

				for _, lesserOrderPod := range lesserOrderPods.Items {
					if lesserOrderPod.Spec.NodeName == "" {
						return false
					}
				}
			}
		}
	}

	return true
}

func ArePodsNeighbors(pod *v1.Pod, peerPod *v1.Pod) bool {
	appGroup, ok := pod.GetLabels()["app-group"]
	if !ok {
		klog.Infof("error getting app-group label for pod %s", pod.Name)
		return false
	}

	peerAppGroup, ok := peerPod.GetLabels()["app-group"]
	if !ok {
		klog.Infof("error getting app-group label for pod %s", peerPod.Name)
		return false
	}

	if appGroup != peerAppGroup {
		return false
	}

	for key, value := range pod.GetLabels() {
		if strings.HasPrefix(key, "chain-") {
			index, err := strconv.ParseFloat(value, 64)
			if err != nil {
				klog.Infof("error parsing chain label value for pod %s", pod.Name)
			}

			peerValue, ok := peerPod.GetLabels()[key]
			if ok {
				peerIndex, err := strconv.ParseFloat(peerValue, 64)
				if err != nil {
					klog.Infof("error parsing chain label value for pod %s", peerPod.Name)
				}
				if int64(math.Abs(index-peerIndex)) == 1 {
					klog.Infof("Pods %s and %s are neighbors", pod.Name, peerPod.Name)
					return true
				}
			}
		}
	}

	return false
}

func GetSharedChainsSlos(pod *v1.Pod, peerPod *v1.Pod) []float64 {
	var chainsSlos []float64

	appGroup, ok := pod.GetLabels()["app-group"]
	if !ok {
		klog.Infof("error getting app-group label for pod %s", pod.Name)
		return chainsSlos
	}

	peerAppGroup, ok := peerPod.GetLabels()["app-group"]
	if !ok {
		klog.Infof("error getting app-group label for pod %s", peerPod.Name)
		return chainsSlos
	}

	if appGroup != peerAppGroup {
		klog.Infof("pods %s and %s do not belong to the same app group", pod.Name, peerPod.Name)
		return chainsSlos
	}

	for key, value := range pod.GetLabels() {
		if strings.HasPrefix(key, "chain-") {
			index, err := strconv.ParseFloat(value, 64)
			if err != nil {
				klog.Infof("error parsing chain label value for pod %s", pod.Name)
				return chainsSlos
			}

			peerValue, ok := peerPod.GetLabels()[key]
			if ok {
				peerIndex, err := strconv.ParseFloat(peerValue, 64)
				if err != nil {
					klog.Infof("error parsing chain label value for pod %s", peerPod.Name)
				}
				if int64(math.Abs(index-peerIndex)) == 1 {
					klog.Infof("pods %s and %s are neighbors", pod.Name, peerPod.Name)

					chainSloAnnotation, ok := pod.GetAnnotations()[key+"-slo"]
					if !ok {
						klog.Infof("error getting %s annotation for pod %s", key+"-slo", pod.Name)
						return chainsSlos
					}

					chainSlo, err := strconv.ParseFloat(chainSloAnnotation, 64)
					if err != nil {
						klog.Infof("error parsing %s annotation for pod %s", key+"-slo", pod.Name)
						return chainsSlos
					}

					chainsSlos = append(chainsSlos, chainSlo)
				}
			}
		}
	}

	return chainsSlos
}

func GetAppCpuUsage(pod *v1.Pod) float64 {
	cpuUsageAnnotation, ok := pod.Annotations["cpu-usage"]
	if !ok {
		klog.Infof("\"cpu-usage\" annotation not found on pod %s", pod.Name)
		return 0.0
	}

	cpuUsage, err := strconv.ParseFloat(cpuUsageAnnotation, 64)
	if err != nil {
		klog.Infof("error parsing \"cpu-usage\" annotation of pod %s", pod.Name)
		return 0.0
	}

	return cpuUsage
}

func GetAppMemoryUsage(pod *v1.Pod) float64 {
	memoryUsageAnnotation, ok := pod.Annotations["memory-usage"]
	if !ok {
		klog.Infof("\"memory-usage\" annotation not found on pod %s", pod.Name)
		return 0.0
	}

	memoryUsage, err := strconv.ParseFloat(memoryUsageAnnotation, 64)
	if err != nil {
		klog.Infof("error parsing \"memory-usage\" annotation of pod %s", pod.Name)
		return 0.0
	}

	return memoryUsage
}

func GetAppRequestsPerSecond(pod *v1.Pod, peerPod *v1.Pod) float64 {
	appGroup, ok := pod.GetLabels()["app-group"]
	if !ok {
		klog.Infof("error getting app-group label for pod %s", pod.Name)
		return 0.0
	}

	peerAppGroup, ok := peerPod.GetLabels()["app-group"]
	if !ok {
		klog.Infof("error getting app-group label for pod %s", peerPod.Name)
		return 0.0
	}

	if appGroup != peerAppGroup {
		klog.Infof("pods %s and %s do not belong to the same app group", pod.Name, peerPod.Name)
		return 0.0
	}

	peerApp, ok := peerPod.GetLabels()["app"]
	if !ok {
		klog.Infof("error getting app label for pod %s", peerPod.Name)
		return 0.0
	}

	rpsAnnotation, ok := pod.Annotations["rps."+peerApp]
	if !ok {
		klog.Infof("\"rps.%s\" annotation not found on pod %s", peerApp, pod.Name)
		return 0.0
	}

	rps, err := strconv.ParseFloat(rpsAnnotation, 64)
	if err != nil {
		klog.Infof("error parsing \"rps.%s\" annotation of pod %s", peerApp, pod.Name)
		return 0.0
	}

	return rps
}

func GetAppTraffic(pod *v1.Pod, peerPod *v1.Pod) float64 {
	appGroup, ok := pod.GetLabels()["app-group"]
	if !ok {
		klog.Infof("error getting app-group label for pod %s", pod.Name)
		return 0.0
	}

	peerAppGroup, ok := peerPod.GetLabels()["app-group"]
	if !ok {
		klog.Infof("error getting app-group label for pod %s", peerPod.Name)
		return 0.0
	}

	if appGroup != peerAppGroup {
		klog.Infof("pods %s and %s do not belong to the same app group", pod.Name, peerPod.Name)
		return 0.0
	}

	peerApp, ok := peerPod.GetLabels()["app"]
	if !ok {
		klog.Infof("error getting app label for pod %s", peerPod.Name)
		return 0.0
	}

	trafficAnnotation, ok := pod.Annotations["traffic."+peerApp]
	if !ok {
		klog.Infof("\"traffic.%s\" annotation not found on pod %s", peerApp, pod.Name)
		return 0.0
	}

	traffic, err := strconv.ParseFloat(trafficAnnotation, 64)
	if err != nil {
		klog.Infof("error parsing \"traffic.%s\" annotation of pod %s", peerApp, pod.Name)
		return 0.0
	}

	return traffic
}

func GetNodeCpuUsage(node *v1.Node) float64 {
	cpuUsageAnnotation, ok := node.Annotations["cpu-usage"]
	if !ok {
		klog.Infof("\"cpu-usage\" annotation not found on node %s", node.Name)
		return 0.0
	}

	cpuUsage, err := strconv.ParseFloat(cpuUsageAnnotation, 64)
	if err != nil {
		klog.Infof("error parsing \"cpu-usage\" annotation of node %s", node.Name)
		return 0.0
	}

	return cpuUsage
}

func GetNodeMemoryUsage(node *v1.Node) float64 {
	memoryUsageAnnotation, ok := node.Annotations["memory-usage"]
	if !ok {
		klog.Infof("\"memory-usage\" annotation not found on node %s", node.Name)
		return 0.0
	}

	memoryUsage, err := strconv.ParseFloat(memoryUsageAnnotation, 64)
	if err != nil {
		klog.Infof("error parsing \"memory-usage\" annotation of node %s", node.Name)
		return 0.0
	}

	return memoryUsage
}

func GetNodeLatency(node *v1.Node, peerNode *v1.Node) float64 {
	latencyAnnotation, ok := node.Annotations["network-latency."+peerNode.Name]
	if !ok {
		klog.Infof("\"network-latency.%s\" annotation not found on node %s", peerNode.Name, node.Name)
		return 0.0
	}

	latency, err := strconv.ParseFloat(latencyAnnotation, 64)
	if err != nil {
		klog.Infof("error parsing \"network-latency.%s\" annotation of node %s", peerNode.Name, node.Name)
		return 0.0
	}

	return latency
}
