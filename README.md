# Scheduler

## Assumptions

The following assumptions are made in this scheduler implementation:

- We don't need `minPodsToEnter` criteria for gann scheduling
- Each deployment will be created with replicas
- If new pods of the same gang ID come, we don't guarantee scheduling
- If no gang ID is given, pods can be preempted
- Pod scheduling is idempotent
- No other scheduler is running

## Testing

Test the scheduler by running the integration test:

```bash
test_scheduler_integration
```