import logging
import shlex
from datetime import datetime

from ocp_resources.cluster_operator import ClusterOperator
from ocp_resources.resource import Resource
from timeout_sampler import TimeoutSampler

from tests.network.constants import IPV4_ADDRESS_SUBNET_PREFIX
from tests.network.flat_overlay.constants import (
    HTTP_SUCCESS_RESPONSE_STR,
)
from utilities.constants import TIMEOUT_5SEC, TIMEOUT_10MIN
from utilities.exceptions import ResourceValueError
from utilities.infra import ExecCommandOnPod, get_node_selector_dict
from utilities.network import compose_cloud_init_data_dict
from utilities.virt import (
    VirtualMachineForTests,
    fedora_vm_body,
    fetch_pid_from_linux_vm,
    running_vm,
    vm_console_run_commands,
)

LOGGER = logging.getLogger(__name__)


def create_flat_overlay_vm(
    vm_name,
    namespace_name,
    nad_name,
    unprivileged_client,
    host_ip_suffix,
    worker_node_hostname=None,
):
    networks = {nad_name: nad_name}
    network_data = {
        "ethernets": {
            "eth1": {"addresses": [f"{IPV4_ADDRESS_SUBNET_PREFIX}.{host_ip_suffix}/24"]},
        }
    }
    cloud_init_data = compose_cloud_init_data_dict(network_data=network_data)
    with VirtualMachineForTests(
        namespace=namespace_name,
        name=vm_name,
        networks=networks,
        interfaces=networks.keys(),
        client=unprivileged_client,
        body=fedora_vm_body(name=vm_name),
        cloud_init_data=cloud_init_data,
        node_selector=get_node_selector_dict(node_selector=worker_node_hostname),
    ) as vm:
        running_vm(vm=vm, wait_for_cloud_init=True)
        yield vm


def get_vm_kubevirt_domain_label(vm):
    kubevirt_domain = f"{Resource.ApiGroup.KUBEVIRT_IO}/domain"
    kubevirt_value = vm.instance.spec.template.metadata.labels.get(kubevirt_domain)
    if kubevirt_value:
        return {kubevirt_domain: kubevirt_value}
    raise ResourceValueError(f"{kubevirt_domain} not found in vm's {vm.name} labels")


def create_ip_block(ip_address, ingress=True):
    network_direction = "from" if ingress else "to"
    return [{network_direction: [{"ipBlock": {"cidr": ip_address}}]}]


def wait_for_network_operator_stable_conditions(reference_time):
    """Wait until ClusterOperator/network has reconciled since `reference_time` and is stable.

    Reconciliation is detected via the `Progressing` condition: only this condition
    reliably transitions during a CNO config change (Available/Degraded stay True/False
    throughout a successful reconcile, so their lastTransitionTime is unreliable).

    Returns once Progressing=False with `lastTransitionTime >= reference_time` and the
    operator is otherwise healthy (Available=True, Degraded=False).
    """
    network_co = ClusterOperator(name="network")

    def _is_reconciled_and_healthy():
        conditions = {cond["type"]: cond for cond in network_co.instance.status.conditions}
        progressing = conditions.get(Resource.Condition.PROGRESSING)
        available = conditions.get(Resource.Condition.AVAILABLE)
        degraded = conditions.get(Resource.Condition.DEGRADED)
        if not (progressing and available and degraded):
            return False
        if progressing["status"] != Resource.Condition.Status.FALSE:
            return False
        if available["status"] != Resource.Condition.Status.TRUE:
            return False
        if degraded["status"] != Resource.Condition.Status.FALSE:
            return False
        progressing_at = datetime.fromisoformat(progressing["lastTransitionTime"].replace("Z", "+00:00"))
        return progressing_at >= reference_time

    LOGGER.info(f"Waiting for ClusterOperator/network to reconcile after {reference_time.isoformat()}.")
    for stable in TimeoutSampler(wait_timeout=TIMEOUT_10MIN, sleep=TIMEOUT_5SEC, func=_is_reconciled_and_healthy):
        if stable:
            return


def get_vm_connection_reply(
    source_vm,
    dst_ip,
    port,
):
    rc, out, _ = source_vm.ssh_exec.run_command(
        command=shlex.split(f"echo -e ' GET http://{dst_ip}:{port} HTTP/1.0\n\n' | nc {dst_ip} {port} -d 1")
    )
    assert not rc, "Could not establish a netcat connection"
    return out.strip()


def start_nc_response_on_vm(flat_l2_port, vm, num_connections):
    vm_console_run_commands(
        vm=vm,
        commands=[
            f'for i in {{1..{num_connections}}}; do echo -e "{HTTP_SUCCESS_RESPONSE_STR}-$i\n\n" | nc '
            f"-lp {flat_l2_port}; done &"
        ],
        verify_commands_output=False,
    )
    fetch_pid_from_linux_vm(vm=vm, process_name="nc")


def is_port_number_available(
    workers_utility_pods,
    worker_node1,
    port,
):
    port_in_use = ExecCommandOnPod(utility_pods=workers_utility_pods, node=worker_node1).exec(
        command=f"ss -tulnap | grep {port}",
        ignore_rc=True,  # return value should be 1 if no socket was found
    )
    return not port_in_use


class NoAvailablePortError(Exception):
    pass
