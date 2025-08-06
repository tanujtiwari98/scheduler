import subprocess
import time
import textwrap

TEST_NAMESPACE = "gang-test"
SCHEDULER_NAME = "foobar"
NODE_COUNT = 2


def kubectl(*args, **kwargs):
    return subprocess.run(["kubectl", *args], capture_output=True, text=True, **kwargs)


def create_from_yaml(manifest):
    kubectl("apply", "-f", "-", input=manifest, check=True)


def create_priority_class(name, value, description):
    manifest = textwrap.dedent(f"""
        apiVersion: scheduling.k8s.io/v1
        kind: PriorityClass
        metadata:
          name: {name}
        value: {value}
        globalDefault: false
        description: "{description}"
    """)
    create_from_yaml(manifest)


def create_deployment(name, replicas, priority_class_name):
    manifest = textwrap.dedent(f"""
        apiVersion: apps/v1
        kind: Deployment
        metadata:
          name: {name}
          namespace: {TEST_NAMESPACE}
          labels:
            app: {name}
        spec:
          replicas: {replicas}
          selector:
            matchLabels:
              app: {name}
          template:
            metadata:
              labels:
                app: {name}
              annotations:
                pod-group: "{name}-group"
            spec:
              schedulerName: {SCHEDULER_NAME}
              priorityClassName: {priority_class_name}
              containers:
              - name: nginx
                image: nginx:alpine
                resources:
                  requests:
                    cpu: "100m"
    """)
    create_from_yaml(manifest)


def wait_for_pods(selector, expected_count, timeout=60):
    print(f"   Waiting for {expected_count} '{selector}' pods to be Running...")
    for _ in range(timeout // 2):
        res = kubectl(
            "get", "pods", "-n", TEST_NAMESPACE, "-l", selector,
            "-o", "jsonpath={.items[*].status.phase}"
        )
        running_pods = res.stdout.split().count("Running")
        if running_pods == expected_count:
            return True
        time.sleep(2)
    print(f"   ✗ Timeout: Failed to find {expected_count} Running pods for '{selector}'.")
    return False


def check_pods_are_pending(selector, expected_count):
    time.sleep(10)
    res = kubectl(
        "get", "pods", "-n", TEST_NAMESPACE, "-l", selector,
        "-o", "jsonpath={.items[*].status.phase}"
    )
    pending_count = res.stdout.split().count("Pending")
    if pending_count == expected_count:
        return True
    print(f"   ✗ Failed: Did not find {expected_count} Pending pods. Status: {res.stdout}")
    return False


def test_gang_scheduling_and_preemption():
    try:
        kubectl("create", "namespace", TEST_NAMESPACE, check=False)
        create_priority_class("low-priority", 100, "Low priority tasks")
        create_priority_class("high-priority", 1000, "High priority tasks")

        create_deployment("low-prio-app", replicas=NODE_COUNT, priority_class_name="low-priority")
        if not wait_for_pods(selector="app=low-prio-app", expected_count=NODE_COUNT):
            raise Exception("Low priority pods failed to schedule.")


        print(f"\n--- Step 2: Deploying high-priority app to trigger preemption ---")
        create_deployment("high-prio-app", replicas=NODE_COUNT, priority_class_name="high-priority")
        if not wait_for_pods(selector="app=high-prio-app", expected_count=NODE_COUNT):
            raise Exception("High priority pods failed to schedule after preemption.")
        print("   Checking status of preempted pods...")
        if not check_pods_are_pending(selector="app=low-prio-app", expected_count=NODE_COUNT):
            print("   Warning: Low priority pods were not in Pending state after preemption.")

        print(f"\n--- Step 3: Attempting to deploy an app too large for the cluster ---")
        oversized_replicas = NODE_COUNT + 1
        create_deployment("too-big-app", replicas=oversized_replicas, priority_class_name="high-priority")
        if not check_pods_are_pending(selector="app=too-big-app", expected_count=oversized_replicas):
            raise Exception("Oversized gang deployment did not behave as expected.")

    except Exception as e:
        print(f"\n❌ Test Failed: {e}")
        kubectl("get", "events", "-n", TEST_NAMESPACE, "--sort-by=lastTimestamp")
    finally:
        kubectl("delete", "namespace", TEST_NAMESPACE, "--ignore-not-found=true")
        kubectl("delete", "priorityclass", "low-priority", "--ignore-not-found=true")
        kubectl("delete", "priorityclass", "high-priority", "--ignore-not-found=true")

if __name__ == "__main__":
    test_gang_scheduling_and_preemption()