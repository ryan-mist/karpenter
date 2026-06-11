export const meta = {
  name: 'dra-test-convergence',
  description: 'Generate DRA tests with 3-agent convergence: generator + design validator + implementation validator',
  whenToUse: 'When you want to generate comprehensive DRA tests for a specific file/component with validation',
  phases: [
    { title: 'Generate', detail: 'Generator agent reads implementation + design and writes tests' },
    { title: 'Validate', detail: 'Two validators check coverage against design and implementation independently' },
    { title: 'Converge', detail: 'Generator incorporates feedback, validators re-check until converged' },
  ],
}

// Repository layout:
//   Design docs:     /Users/ryanmist/Desktop/karpenter-plan  (branch consumable-capacity-plan)
//   Implementation:  /Users/ryanmist/Desktop/karp/karpenter  (branch consumable-capacity)
//
// Tests live alongside implementation code in the implementation worktree.

const IMPL_ROOT = '/Users/ryanmist/Desktop/karp/karpenter'
const DESIGN_ROOT = '/Users/ryanmist/Desktop/karpenter-plan'

const TARGET = args?.target || 'pkg/scheduling/dynamicresources/allocator.go'
const MAX_ROUNDS = args?.maxRounds || 3

const REPO_CONTEXT = `
## Repository Layout

This project uses TWO git worktrees of the same Karpenter repo:

- **Design docs:** ${DESIGN_ROOT} (branch consumable-capacity-plan)
  - designs/dra/scheduling.md — Authoritative allocator design doc
  - designs/dra/consumable-capacity.md — Upstream KEP-5075 semantics
  - designs/dra/consumable-capacity-integration.md — Karpenter integration design
  - designs/dra/consumable-capacity-notes.md — Scoping decisions, known bugs

- **Implementation + Tests:** ${IMPL_ROOT} (branch consumable-capacity)
  - pkg/scheduling/dynamicresources/ — Core DRA allocator code and tests
  - pkg/controllers/dynamicresources/ — Device allocation controller
  - pkg/cloudprovider/ — Cloud provider interface

When reading files:
- Implementation/tests: use paths relative to ${IMPL_ROOT}
- Design docs: use paths relative to ${DESIGN_ROOT}
`

const GENERATOR_PROMPT = `You are a DRA test generator for the Karpenter project. Your job is to write comprehensive Ginkgo tests.

${REPO_CONTEXT}

## Target
Generate tests for: ${IMPL_ROOT}/${TARGET}

## CRITICAL: Context Management
You MUST be efficient with your context window. Follow this exact reading strategy:
1. Read ONLY the target file first: ${IMPL_ROOT}/${TARGET}
2. Read the existing test file for the target (same name with _test.go suffix) — use offset/limit to read only the first 200 lines to understand imports and helpers
3. Do NOT read allocator_test.go unless the target IS allocator.go — it is 3000+ lines and will exhaust your context
4. Use grep to find specific helper functions instead of reading entire files
5. For design context, read ONLY the relevant sections using grep first to locate line numbers, then read with offset/limit

## Instructions
1. Read the target implementation file
2. Identify all code paths, branches, and error conditions
3. Read the test helpers you need (grep for specific function names)
4. Generate tests that:
   - Use Ginkgo v2 (. "github.com/onsi/ginkgo/v2") and Gomega (. "github.com/onsi/gomega")
   - Use DescribeTable with Entry() wherever multiple cases test the same behavior with different inputs
   - Cover ALL code paths: happy paths, error paths, edge cases, boundary conditions
   - Reuse existing test helpers (makeNodeClaim, makeTemplate, makeClaim, deviceID, etc.)
   - Follow the exact same package_test pattern (package dynamicresources_test)
   - Group related tests under Describe/Context blocks
5. ADD tests to the existing test file for the target — return the COMPLETE additions (new Describe blocks only, not the whole file)

## CRITICAL: Minimize Test Count — Maximize Fixture Density
The goal is FEWER tests with RICHER fixtures, not more tests with thin fixtures.

**Rules:**
1. Put ALL fields on the BeforeEach fixture. If a struct has Attributes, Capacity, and AllowMultipleAllocations,
   put all three on the shared fixture — don't create separate tests for each field.
2. Merge assertions into existing It() blocks that already exercise the same code path.
   Only create a new It() when testing a GENUINELY DIFFERENT code path (different error, different branch).
3. A test for "capacity is converted" and a test for "attributes are converted" should be ONE test
   ("should convert devices with attributes, capacity, and AllowMultipleAllocations") because they
   exercise the same Devices() method on the same fixture.
4. Separate It() blocks are justified ONLY for: distinct error paths, nil/empty edge cases that
   exercise a different branch, or behaviors that require a different fixture setup (e.g. a zonal
   nodeSelector vs allNodes).
5. NEVER create a separate test just to assert a single field when you could add that assertion
   to an existing test that already builds the same object.

**Bad (5 tests for 1 code path):**
- "should convert attributes" / "should convert capacity" / "should copy AllowMultiple" / "should handle both" / "should cache"

**Good (2 tests):**
- "should convert devices with attributes, capacity, and AllowMultipleAllocations" (one rich fixture, all field assertions inline)
- "should cache devices on repeated calls" (distinct behavior: caching)

## Available Test Helpers (from pool_test.go and attributebindings_test.go)
- makeAPISlice(name, driver, pool string, opts ...func(*resourcev1.ResourceSlice)) — builds an API ResourceSlice
- withAllNodes() — marks slice as accessible from all nodes
- withAPIDevices(names ...string) — adds plain devices
- withAPIDevicesWithAttrs(specs ...apiDeviceSpec) — adds devices with attributes
- withGeneration(gen, sliceCount int64) — sets pool generation
- deviceWithAttrs(name string, attrs map[QualifiedName]DeviceAttribute) — builds a device spec
- deviceID(driver, pool, device string) DeviceID — builds a device ID

## Available Test Helpers (from allocator_test.go)
- makeNodeClaim(itNames ...string) *fakeNodeClaim
- makeNodeClaimWithTemplates(itName string, templates ...*cloudprovider.ResourceSliceTemplate)
- makeTemplate(driver, pool string, deviceNames ...string) *cloudprovider.ResourceSliceTemplate
- makeTemplateWithAttrs(driver, pool string, specs ...apiDeviceSpec) *cloudprovider.ResourceSliceTemplate
- makeClaim(name string, requests ...resourcev1.DeviceRequest) *resourcev1.ResourceClaim
- makeClaimWithConstraints(name string, constraints []DeviceConstraint, requests ...) *resourcev1.ResourceClaim
- exactRequest(name, className string, count int64) resourcev1.DeviceRequest
- exactRequestWithSelector(name, className string, count int64, expr string) resourcev1.DeviceRequest

## Output Format
Return ONLY the new Go test code to ADD (new Describe/Context blocks). Include any new imports needed.
Do NOT return the full file — just the additions.`

const DESIGN_VALIDATOR_PROMPT = `You are a DRA test coverage validator focused on DESIGN COMPLETENESS.

${REPO_CONTEXT}

## CRITICAL: Context Management
- Read ONLY the design doc sections relevant to the target. Use grep to find section headings first.
- Do NOT read entire design docs — use offset/limit to read specific sections.
- Focus on the BEHAVIORS described in the design that should be tested.

## Your Task
1. Read the test code provided below.
2. Read the relevant design doc sections (use grep + offset/limit):
   - ${DESIGN_ROOT}/designs/dra/scheduling.md
   - ${DESIGN_ROOT}/designs/dra/consumable-capacity-integration.md (if target touches consumable capacity)
3. Identify design behaviors NOT covered by the tests.

## Target
The tests are for: ${IMPL_ROOT}/${TARGET}

## Tests to Validate
{TESTS}

## Output Format
Return a JSON object with:
- "covered": array of design behaviors that ARE tested
- "missing": array of design behaviors NOT tested (be specific, reference design doc sections)
- "verdict": "pass" or "needs_work"
- "feedback": specific instructions about what to add (keep under 500 words)`

const IMPL_VALIDATOR_PROMPT = `You are a DRA test coverage validator focused on IMPLEMENTATION COMPLETENESS.

${REPO_CONTEXT}

## CRITICAL: Context Management
- Read ONLY the target implementation file.
- Do NOT read test helpers or other files unless absolutely necessary.
- Compare test code against implementation code paths.

## Your Task
1. Read the test code provided below.
2. Read the implementation: ${IMPL_ROOT}/${TARGET}
3. Identify code paths NOT exercised by the tests.

## Target
The tests are for: ${IMPL_ROOT}/${TARGET}

## Tests to Validate
{TESTS}

## Output Format
Return a JSON object with:
- "covered_paths": array of code paths that ARE tested
- "uncovered_paths": array of code paths NOT tested (reference line numbers)
- "verdict": "pass" or "needs_work"
- "feedback": specific instructions about what to add (keep under 500 words)`

const CONVERGENCE_PROMPT = `You are a DRA test generator incorporating validator feedback.

${REPO_CONTEXT}

## Target
Tests for: ${IMPL_ROOT}/${TARGET}

## CRITICAL: Context Management
- Do NOT re-read files you already know about. Use the feedback directly.
- Read ONLY specific lines referenced in the feedback (use offset/limit).
- Keep your output focused on the NEW tests to add.

## Previous Tests
{PREVIOUS_TESTS}

## Design Validator Feedback
{DESIGN_FEEDBACK}

## Implementation Validator Feedback
{IMPL_FEEDBACK}

## Instructions
1. Address the gaps identified by both validators.
2. Read ONLY the specific source lines referenced in the feedback.
3. Produce ADDITIONAL test code (new Describe/Context blocks only).
4. Use the same patterns: Ginkgo v2, Gomega, DescribeTable where appropriate, reuse existing helpers.

## Output Format
Return the COMPLETE updated test additions (previous + new). Do NOT return the full original test file.`

const TESTS_SCHEMA = {
  type: 'object',
  properties: {
    code: { type: 'string', description: 'Go test code (new Describe blocks to add)' },
    summary: { type: 'string', description: 'Brief summary of what was generated (under 100 words)' },
  },
  required: ['code', 'summary'],
}

const VALIDATION_SCHEMA = {
  type: 'object',
  properties: {
    covered: { type: 'array', items: { type: 'string' } },
    missing: { type: 'array', items: { type: 'string' } },
    covered_paths: { type: 'array', items: { type: 'string' } },
    uncovered_paths: { type: 'array', items: { type: 'string' } },
    verdict: { type: 'string', enum: ['pass', 'needs_work'] },
    feedback: { type: 'string' },
  },
  required: ['verdict', 'feedback'],
}

// Phase 1: Generate initial tests
phase('Generate')
log(`Generating tests for ${TARGET} (impl: ${IMPL_ROOT}, design: ${DESIGN_ROOT})`)

const initial = await agent(GENERATOR_PROMPT, {
  label: 'generator:initial',
  schema: TESTS_SCHEMA,
})

if (!initial) {
  log('Generator failed to produce output')
  return { error: 'Generator failed' }
}

log(`Generated initial tests: ${initial.summary}`)

let currentTests = initial.code
let round = 0
let converged = false
let lastDesignResult = null
let lastImplResult = null

while (!converged && round < MAX_ROUNDS) {
  round++

  // Phase 2: Validate in parallel
  phase('Validate')
  log(`Validation round ${round}/${MAX_ROUNDS}...`)

  const [designResult, implResult] = await parallel([
    () => agent(
      DESIGN_VALIDATOR_PROMPT.replace('{TESTS}', currentTests),
      { label: `design-validator:r${round}`, schema: VALIDATION_SCHEMA }
    ),
    () => agent(
      IMPL_VALIDATOR_PROMPT.replace('{TESTS}', currentTests),
      { label: `impl-validator:r${round}`, schema: VALIDATION_SCHEMA }
    ),
  ])

  lastDesignResult = designResult
  lastImplResult = implResult

  const designVerdict = designResult?.verdict || 'pass'
  const implVerdict = implResult?.verdict || 'pass'

  log(`Round ${round} — Design: ${designVerdict} (${designResult?.missing?.length || 0} gaps), Implementation: ${implVerdict} (${implResult?.uncovered_paths?.length || 0} gaps)`)

  if (designVerdict === 'pass' && implVerdict === 'pass') {
    converged = true
    log('Both validators passed — tests are converged.')
    break
  }

  // Phase 3: Converge — generator incorporates feedback
  phase('Converge')
  log(`Incorporating feedback from round ${round}...`)

  const convergencePrompt = CONVERGENCE_PROMPT
    .replace('{PREVIOUS_TESTS}', currentTests)
    .replace('{DESIGN_FEEDBACK}', designResult?.feedback || 'No feedback — design coverage is acceptable.')
    .replace('{IMPL_FEEDBACK}', implResult?.feedback || 'No feedback — implementation coverage is acceptable.')

  const update = await agent(convergencePrompt, {
    label: `generator:converge-r${round}`,
    schema: TESTS_SCHEMA,
  })

  if (update) {
    currentTests = update.code
    log(`Round ${round} update: ${update.summary}`)
  } else {
    log(`Generator produced no update in round ${round}, stopping.`)
    break
  }
}

return {
  target: TARGET,
  tests: currentTests,
  rounds: round,
  converged,
  finalDesignFeedback: lastDesignResult?.feedback,
  finalImplFeedback: lastImplResult?.feedback,
  designMissing: lastDesignResult?.missing,
  implUncovered: lastImplResult?.uncovered_paths,
}
