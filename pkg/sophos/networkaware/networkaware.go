package networkaware

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
	Name = "NetworkAware"
)

type NetworkAware struct {
	handle framework.Handle
}

var _ = framework.ScorePlugin(&NetworkAware{})

func (pl *NetworkAware) Name() string {
	return Name
}

func (pl *NetworkAware) Score(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
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

		for _, peerPod := range pods.Items {
			score -= int64(sophos.GetNodeLatency(node.Node(), clusterNode.Node()) * sophos.GetAppTraffic(pod, &peerPod))
		}
	}

	return score, nil
}

func (pl *NetworkAware) ScoreExtensions() framework.ScoreExtensions {
	return pl
}

func (pl *NetworkAware) NormalizeScore(_ context.Context, _ *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
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

func New(_ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	pl := &NetworkAware{
		handle: h,
	}
	return pl, nil
}
