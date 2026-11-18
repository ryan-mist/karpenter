/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package overlay_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/apis/v1alpha1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/overlay"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeoverlay"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	testv1alpha1 "sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var (
	ctx           context.Context
	env           *test.Environment
	cloudProvider *fake.CloudProvider
	store         *nodeoverlay.InstanceTypeStore
	controller    *nodeoverlay.Controller
	overlaid      cloudprovider.CloudProvider
)

type erroringCloudProvider struct {
	*fake.CloudProvider
	err error
}

func (e *erroringCloudProvider) GetInstanceTypes(context.Context, *v1.NodePool) ([]*cloudprovider.InstanceType, error) {
	return nil, e.err
}

func TestOverlay(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "CloudProvider Overlay Suite")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(test.WithCRDs(apis.CRDs...), test.WithCRDs(testv1alpha1.CRDs...))
	cloudProvider = fake.NewCloudProvider()
	store = nodeoverlay.NewInstanceTypeStore()
	controller = nodeoverlay.NewController(env.Clock, env.Client, cloudProvider, store, state.NewCluster(env.Clock, env.Client, cloudProvider))
	overlaid = overlay.Decorate(cloudProvider, env.Client, store)
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = Describe("CloudProvider Overlay", func() {
	It("should add NodePool context to cloud provider errors", func() {
		providerErr := errors.New("provider unavailable")
		nodePool := &v1.NodePool{}
		nodePool.Name = "default"
		decorated := overlay.Decorate(&erroringCloudProvider{
			CloudProvider: fake.NewCloudProvider(),
			err:           providerErr,
		}, nil, nil)

		_, err := decorated.GetInstanceTypes(options.ToContext(context.Background(), &options.Options{}), nodePool)
		Expect(err).To(MatchError(fmt.Sprintf("getting cloud provider instance types, %s (NodePool=%s)", providerErr, nodePool.Name)))
		Expect(errors.Is(err, providerErr)).To(BeTrue())
	})
	It("should add empty NodePool context when the NodePool is nil", func() {
		providerErr := errors.New("provider unavailable")
		decorated := overlay.Decorate(&erroringCloudProvider{
			CloudProvider: fake.NewCloudProvider(),
			err:           providerErr,
		}, nil, nil)

		_, err := decorated.GetInstanceTypes(options.ToContext(context.Background(), &options.Options{}), nil)
		Expect(err).To(MatchError(fmt.Sprintf("getting cloud provider instance types, %s (NodePool=)", providerErr)))
		Expect(errors.Is(err, providerErr)).To(BeTrue())
	})
	Context("GetInstanceTypes", func() {
		var nodePool *v1.NodePool
		BeforeEach(func() {
			ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodeOverlay: new(true)}}))
			cloudProvider.Reset()
			store.Reset()
			cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{
				fake.NewInstanceType("overlaid-instance-type", fake.WithOfferings(cloudprovider.Offering{
					Available:    true,
					Compatible:   true,
					Requirements: scheduling.NewLabelRequirements(map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeSpot, corev1.LabelTopologyZone: "test-zone-1"}),
					Price:        1.0,
				})),
				fake.NewInstanceType("other-instance-type", fake.WithOfferings(cloudprovider.Offering{
					Available:    true,
					Compatible:   true,
					Requirements: scheduling.NewLabelRequirements(map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeSpot, corev1.LabelTopologyZone: "test-zone-1"}),
					Price:        2.0,
				})),
			}
			nodePool = test.NodePool()
			ExpectApplied(ctx, env.Client, nodePool)
		})
		AfterEach(func() {
			ExpectCleanedUp(ctx, env.Client)
		})
		priceOf := func(its []*cloudprovider.InstanceType, name string) float64 {
			it, ok := lo.Find(its, func(it *cloudprovider.InstanceType) bool { return it.Name == name })
			Expect(ok).To(BeTrue(), name)
			Expect(it.Offerings).To(HaveLen(1))
			return it.Offerings[0].Price
		}
		It("should return an unevaluated NodePool error until the NodeOverlay controller has evaluated the NodePool", func() {
			its, err := overlaid.GetInstanceTypes(ctx, nodePool)
			Expect(cloudprovider.IsUnevaluatedNodePoolError(err)).To(BeTrue())
			Expect(err).To(MatchError(ContainSubstring("applying nodeoverlays")))
			Expect(its).To(BeEmpty())
		})
		It("should apply NodeOverlays once the NodePool has been evaluated", func() {
			ExpectApplied(ctx, env.Client, test.NodeOverlay(v1alpha1.NodeOverlay{
				Spec: v1alpha1.NodeOverlaySpec{
					Requirements: []v1alpha1.NodeSelectorRequirement{{
						Key:      corev1.LabelInstanceTypeStable,
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"overlaid-instance-type"},
					}},
					Price: new("0.25"),
				},
			}))
			ExpectReconciled(ctx, controller, reconcile.Request{})

			its, err := overlaid.GetInstanceTypes(ctx, nodePool)
			Expect(err).ToNot(HaveOccurred())
			Expect(its).To(HaveLen(2))
			Expect(priceOf(its, "overlaid-instance-type")).To(BeNumerically("==", 0.25))
			Expect(priceOf(its, "other-instance-type")).To(BeNumerically("==", 2.0))

			// The overlay is applied to copies, so the cloud provider's own instance types keep their price
			raw, err := cloudProvider.GetInstanceTypes(ctx, nodePool)
			Expect(err).ToNot(HaveOccurred())
			Expect(priceOf(raw, "overlaid-instance-type")).To(BeNumerically("==", 1.0))
		})
		It("should return instance types unchanged when the NodePool has no matching NodeOverlays", func() {
			ExpectReconciled(ctx, controller, reconcile.Request{})

			its, err := overlaid.GetInstanceTypes(ctx, nodePool)
			Expect(err).ToNot(HaveOccurred())
			Expect(priceOf(its, "overlaid-instance-type")).To(BeNumerically("==", 1.0))
			Expect(priceOf(its, "other-instance-type")).To(BeNumerically("==", 2.0))
		})
		It("should bypass the overlay store when the NodeOverlay feature gate is disabled", func() {
			ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodeOverlay: new(false)}}))
			// The NodePool hasn't been evaluated, which would be an error with the gate enabled
			its, err := overlaid.GetInstanceTypes(ctx, nodePool)
			Expect(err).ToNot(HaveOccurred())
			Expect(its).To(HaveLen(2))
			Expect(priceOf(its, "overlaid-instance-type")).To(BeNumerically("==", 1.0))
		})
	})
})
