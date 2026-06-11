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

## Instructions
1. Read the target implementation file: ${IMPL_ROOT}/${TARGET}
2. Read the design docs:
   - ${DESIGN_ROOT}/designs/dra/scheduling.md
   - ${DESIGN_ROOT}/designs/dra/consumable-capacity-integration.md (if the target touches consumable capacity)
3. Read existing tests in the same directory to understand patterns, helpers, and style:
   - Look at ${IMPL_ROOT}/pkg/scheduling/dynamicresources/allocator_test.go for test helpers
   - Look at ${IMPL_ROOT}/pkg/scheduling/dynamicresources/pool_test.go for DescribeTable examples
   - Look at ${IMPL_ROOT}/pkg/scheduling/dynamicresources/constraint_test.go for constraint testing patterns
4. **PREFER adding tests to existing test files** rather than creating new ones. If there is already a test file
   for the target (e.g., allocator_test.go for allocator.go), ADD your tests to that file. Only create a new
   test file if no existing file covers the target or if the new tests represent a fundamentally different
   concern that warrants its own file.
5. Generate comprehensive tests that:
   - Use Ginkgo v2 (. "github.com/onsi/ginkgo/v2") and Gomega (. "github.com/onsi/gomega")
   - Use DescribeTable with Entry() wherever multiple cases test the same behavior with different inputs
   - Cover ALL code paths: happy paths, error paths, edge cases, boundary conditions
   - Reuse existing test helpers (makeNodeClaim, makeTemplate, makeClaim, etc.) from the existing test files
   - Follow the exact same package_test pattern (package dynamicresources_test)
   - Group related tests under Describe/Context blocks
   - Name test cases to describe the behavior being tested, not the implementation detail

## Feature Gating Considerations
DRA tests require Kubernetes 1.30+ (the DynamicResourceAllocation feature gate). Consumable capacity
(KEP-5075) may require an even newer Kubernetes version or a separate feature gate that isn't available
in all environments.

When generating tests:
- For integration tests (test/suites/dra/), check how the existing suite handles DRA gating. The suite
  already runs against a DRA-enabled cluster, so basic DRA tests don't need extra gating.
- For consumable capacity tests specifically, check if there is a version/feature gate check needed.
  Look at the pattern in test/pkg/environment/common/expectations.go where version.Minor is checked
  and Skip() is called for unsupported versions. If consumable capacity needs gating beyond the existing
  DRA gate, add a similar Skip() guard.
- For unit tests (pkg/scheduling/dynamicresources/*_test.go), feature gating is not relevant since
  they test internal logic without a real cluster.

## Output Format
Return ONLY the Go test code in the "code" field. No explanations outside the JSON.
If there are already tests for this file, generate ADDITIONAL tests that cover cases not yet tested.
When adding to an existing file, return the COMPLETE file contents (existing + new tests merged).
Include the copyright header and all necessary imports.`

const DESIGN_VALIDATOR_PROMPT = `You are a DRA test coverage validator focused on DESIGN COMPLETENESS.

${REPO_CONTEXT}

## Your Task
1. Read the test code provided below.
2. From ONLY the tests, reverse-engineer what you believe the design/specification must be.
3. Read the actual design docs:
   - ${DESIGN_ROOT}/designs/dra/scheduling.md
   - ${DESIGN_ROOT}/designs/dra/consumable-capacity-integration.md
4. Compare your inferred design with the actual design.
5. Identify any design behaviors, invariants, or edge cases that are NOT covered by the tests.
6. Check whether tests are being added to existing test files (preferred) or creating unnecessary new files.
7. For integration tests touching consumable capacity, verify that appropriate feature gating is in place.
   DRA requires k8s 1.30+. Consumable capacity (KEP-5075) may need a newer version or additional gate.
   Check how existing DRA tests handle this (look at test/suites/dra/suite_test.go and the
   version.Minor Skip() pattern in test/pkg/environment/common/expectations.go).

## Target
The tests are for: ${IMPL_ROOT}/${TARGET}

## Tests to Validate
{TESTS}

## Output Format
Return a JSON object with these fields:
- "covered": array of design behaviors that ARE tested
- "missing": array of design behaviors that are NOT tested (be specific about what scenario is missing, reference design doc sections)
- "verdict": "pass" or "needs_work"
- "feedback": specific instructions for the generator about what to add, referencing exact design doc sections and behaviors. Include feedback about file placement (existing vs new file) and feature gating if relevant.`

const IMPL_VALIDATOR_PROMPT = `You are a DRA test coverage validator focused on IMPLEMENTATION COMPLETENESS.

${REPO_CONTEXT}

## Your Task
1. Read the test code provided below.
2. From ONLY the tests, reverse-engineer what you believe the implementation does.
3. Read the actual implementation: ${IMPL_ROOT}/${TARGET}
4. Compare your inferred behavior with the actual code paths.
5. Identify any code paths, branches, error conditions, or edge cases that are NOT exercised by the tests.

## Target
The tests are for: ${IMPL_ROOT}/${TARGET}

## Tests to Validate
{TESTS}

## Output Format
Return a JSON object with these fields:
- "covered_paths": array of code paths/branches that ARE tested
- "uncovered_paths": array of code paths/branches that are NOT tested (reference line numbers from the implementation file)
- "verdict": "pass" or "needs_work"
- "feedback": specific instructions for the generator about what to add, referencing exact functions, line numbers, and branch conditions`

const CONVERGENCE_PROMPT = `You are a DRA test generator incorporating validator feedback. Your job is to produce a COMPLETE updated test file that addresses the gaps identified by both validators.

${REPO_CONTEXT}

## Target
Tests for: ${IMPL_ROOT}/${TARGET}

## Previous Tests (complete file)
{PREVIOUS_TESTS}

## Design Validator Feedback
{DESIGN_FEEDBACK}

## Implementation Validator Feedback
{IMPL_FEEDBACK}

## Instructions
1. Read the feedback from both validators carefully.
2. Read the relevant source files to understand the gaps:
   - Implementation: ${IMPL_ROOT}/${TARGET}
   - Design: ${DESIGN_ROOT}/designs/dra/scheduling.md
3. Check the existing test file for the target — prefer adding to it rather than creating a new file.
4. Produce a COMPLETE updated test file that includes both the previous tests AND new tests addressing the gaps.
5. Use the same patterns: Ginkgo v2, Gomega, DescribeTable where appropriate, reuse existing helpers.
6. Use DescribeTable for any set of cases that test the same behavior with different inputs.
7. For consumable capacity tests in the integration suite (test/suites/dra/), add a version/feature
   gate Skip() guard if the consumable capacity feature requires a newer k8s version than 1.30. Check
   existing patterns in the codebase first (e.g., version.Minor checks with Skip()).

## Output Format
Return the COMPLETE Go test file in the "code" field (not just additions — the full file including previous tests and new ones).`

const TESTS_SCHEMA = {
  type: 'object',
  properties: {
    code: { type: 'string', description: 'The complete Go test file content' },
    summary: { type: 'string', description: 'Brief summary of what was generated/added' },
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
