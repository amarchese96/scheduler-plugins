package loadawareresourcesbalancedallocation

import (
	"context"
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/pkg/sophos"
)

const (
	Name = "LoadAwareResourcesBalancedAllocation"
)

type LoadAwareResourcesBalancedAllocation struct {
	handle framework.Handle
}

var _ = framework.ScorePlugin(&LoadAwareResourcesBalancedAllocation{})

func (pl *LoadAwareResourcesBalancedAllocation) Name() string {
	return Name
}

func (pl *LoadAwareResourcesBalancedAllocation) Score(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	klog.Infof("Scoring node %q for pod %q", nodeName, pod.Name)

	node, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("error getting info for node %q: %v", nodeName, err))
	}

	cpuUsageRatio := -(sophos.GetAppCpuUsage(ctx, pl.handle, pod) + sophos.GetNodeCpuUsage(node.Node())) * 100 / float64(node.Allocatable.MilliCPU)
	memoryUsageRatio := -(sophos.GetAppMemoryUsage(ctx, pl.handle, pod) + sophos.GetNodeMemoryUsage(node.Node())) * 100 / float64(node.Allocatable.Memory)

	std := math.Abs((cpuUsageRatio - memoryUsageRatio) / 2)
	score := int64((1 - std) * float64(framework.MaxNodeScore))

	return score, nil
}

func (pl *LoadAwareResourcesBalancedAllocation) ScoreExtensions() framework.ScoreExtensions {
	return pl
}

func (pl *LoadAwareResourcesBalancedAllocation) NormalizeScore(_ context.Context, _ *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
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
	pl := &LoadAwareResourcesBalancedAllocation{
		handle: handle,
	}
	return pl, nil
}
