package common

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

// CreateJobSubmitterSidecar creates a sidecar container for job submission in SidecarMode
func CreateJobSubmitterSidecar(rayJobInstance *rayv1.RayJob, rayClusterInstance *rayv1.RayCluster) (corev1.Container, error) {
	// Get the job submission command
	k8sJobCommand, err := GetK8sJobCommand(rayJobInstance)
	if err != nil {
		return corev1.Container{}, fmt.Errorf("failed to get job command: %w", err)
	}

	// Create a script that waits for Ray head to be ready before submitting the job
	waitAndSubmitScript := fmt.Sprintf(`
#!/bin/bash
set -e

echo "Waiting for Ray head to be ready..."
# Wait for Ray dashboard to be available
until curl -s localhost:8265/api/version > /dev/null 2>&1; do
    echo "Ray dashboard not ready yet, waiting..."
    sleep 2
done

echo "Ray head is ready, submitting job..."
%s
`, strings.Join(k8sJobCommand, " "))

	// Create the sidecar container
	sidecarContainer := corev1.Container{
		Name: "ray-job-submitter-sidecar",
		// Use the same image as Ray head to avoid version mismatch issues
		Image: rayClusterInstance.Spec.HeadGroupSpec.Template.Spec.Containers[utils.RayContainerIndex].Image,
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("200Mi"),
			},
		},
		// Set environment variables for sidecar container
		Env: []corev1.EnvVar{
			{
				Name:  "PYTHONUNBUFFERED",
				Value: "1",
			},
			{
				Name:  "RAY_ADDRESS",
				Value: "http://localhost:8265",
			},
			{
				Name:  "RAY_JOB_SUBMISSION_ID",
				Value: rayJobInstance.Status.JobId,
			},
		},
		// Use bash to execute the wait and submit script
		Command: []string{"/bin/bash"},
		Args:    []string{"-c", waitAndSubmitScript},
	}

	return sidecarContainer, nil
}

// GetSidecarContainerStatus checks if the sidecar container has finished successfully
func GetSidecarContainerStatus(pod *corev1.Pod, sidecarContainerName string) (bool, bool, error) {
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.Name == sidecarContainerName {
			if containerStatus.State.Terminated != nil {
				// Container has finished
				return true, containerStatus.State.Terminated.ExitCode == 0, nil
			}
			// Container is still running
			return false, false, nil
		}
	}
	return false, false, fmt.Errorf("sidecar container %s not found in pod", sidecarContainerName)
}
