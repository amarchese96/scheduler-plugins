package networksloaware

import (
	"context"
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/pkg/sophos"
)

const (
	Name = "NetworkSloAware"
)

type NetworkSloAware struct {
	handle framework.Handle
}

var _ = framework.PreFilterPlugin(&NetworkSloAware{})
var _ = framework.ScorePlugin(&NetworkSloAware{})

func (pl *NetworkSloAware) Name() string {
	return Name
}

func (pl *NetworkSloAware) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	if sophos.AreLesserOrderPodsScheduled(ctx, pl.handle, pod) {
		klog.Infof("pod %s ready to be scheduled", pod.Name)
		return nil, framework.NewStatus(framework.Success, fmt.Sprintf("pod %s ready to be scheduled", pod.Name))
	}

	klog.Infof("pod %s not ready to be scheduled", pod.Name)
	return nil, framework.NewStatus(framework.Code(framework.Queue), fmt.Sprintf("pod %s not ready to be scheduled", pod.Name))
}

func (pl *NetworkSloAware) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

func (pl *NetworkSloAware) EventsToRegister() []framework.ClusterEventWithHint {
	return []framework.ClusterEventWithHint{
		{Event: framework.ClusterEvent{Resource: framework.Pod, ActionType: framework.Update}, QueueingHintFn: pl.isSchedulableAfterPodChange},
	}
}

func (pl *NetworkSloAware) isSchedulableAfterPodChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (framework.QueueingHint, error) {
	klog.Infof("trying to renqueue pod %s", pod.Name)
	return framework.Queue, nil
}

func (pl *NetworkSloAware) Score(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	klog.Infof("scoring node %q for pod %q", nodeName, pod.Name)
	var score int64

	node, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("error getting info for node %q: %v", nodeName, err))
	}

	clusterNodes, err := pl.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("error getting cluster nodes info: %v", err))
	}

	for _, clusterNode := range clusterNodes {
		pods, err := pl.handle.ClientSet().CoreV1().Pods(pod.Namespace).List(ctx, metav1.ListOptions{
			FieldSelector: "spec.nodeName=" + clusterNode.Node().Name,
		})
		if err != nil {
			return 0, framework.NewStatus(framework.Error, fmt.Sprintf("error getting pods scheduled on node %q", clusterNode.Node().Name))
		}

		nodeLatency := sophos.GetNodeLatency(node.Node(), clusterNode.Node())

		for _, peerPod := range pods.Items {
			if sophos.ArePodsNeighbors(pod, &peerPod) {
				chainsSlos := sophos.GetSharedChainsSlos(pod, &peerPod)
				rps := sophos.GetAppRequestsPerSecond(ctx, pl.handle, pod, &peerPod)

				for _, chainSlo := range chainsSlos {
					score -= int64(nodeLatency * (rps + 100) / chainSlo)
				}
			}
		}
	}

	return score, nil
}

func (pl *NetworkSloAware) ScoreExtensions() framework.ScoreExtensions {
	return pl
}

func (pl *NetworkSloAware) NormalizeScore(_ context.Context, _ *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	// Find highest and lowest scores.
	var highest int64 = -math.MaxInt64
	var lowest int64 = math.MaxInt64
	for _, nodeScore := range scores {
		if nodeScore.Score > highest {
			highest = nodeScore.Score
		}
		if nodeScore.Score < lowest {
			lowest = nodeScore.Score
		}
	}

	// Transform the highest to the lowest score range to fit the framework's min to max node score range.
	oldRange := highest - lowest
	newRange := framework.MaxNodeScore - framework.MinNodeScore
	for i, nodeScore := range scores {
		if oldRange == 0 {
			scores[i].Score = framework.MinNodeScore
		} else {
			scores[i].Score = ((nodeScore.Score - lowest) * newRange / oldRange) + framework.MinNodeScore
		}
		klog.Infof("Original score of node %q for pod %q: %d", scores[i].Name, pod.Name, nodeScore.Score)
		klog.Infof("Normalized score of node %q for pod %q: %d", scores[i].Name, pod.Name, scores[i].Score)
	}

	return nil
}

func New(_ context.Context, _ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	pl := &NetworkSloAware{
		handle: handle,
	}
	return pl, nil
}
