#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# teardown.sh — destroy the entire audit environment.
#
#   *** NOT ARMED. Nothing in this environment self-terminates. ***
#
# The three instances are deliberately persistent: the audit runs over days,
# so there is no auto-shutdown, no dead-man switch and no idle timer. They
# bill continuously until someone runs this script.
#
#   Approximate cost, us-east-2 on-demand:
#     c8g.4xlarge  (clickbench)   ~$0.638/hr
#     i8g.xlarge   (oltp-server)  ~$0.322/hr
#     c8g.2xlarge  (oltp-client)  ~$0.319/hr
#     EBS root (500 GB + 2 x 100 GB gp3) ~$0.096/hr
#     EBS gp2 500 GiB reference data volume ~$0.068/hr (~$50/mo)
#     3 Elastic IPs (while associated) ~$0.015/hr
#     ------------------------------------------
#     TOTAL                       ~$1.46/hr  ~=  $35/day  ~=  $245/week
#
#   The gp2 reference volume has DeleteOnTermination=false: it survives
#   instance termination on purpose (it holds hours of load work), which also
#   means it keeps billing after everything else is gone. This script deletes
#   it; if you tear down by hand, delete it by hand.
#
# RUN THIS FROM A MACHINE WITH AWS CREDENTIALS FOR THE OWNING ACCOUNT.
# It is intentionally NOT runnable from the audit instances themselves: they
# have no IAM role and no credentials, by design.
#
# WHAT IT DELETES
#   3 instances, 3 Elastic IP allocations, the security group, the keypair.
#   EBS root volumes are delete-on-termination and go with the instances.
#   The instance-store NVMe on oltp-server (all four datadirs) is destroyed
#   the moment that instance stops — it is not recoverable and is not backed
#   up anywhere.
#
# USAGE
#   ./teardown.sh --dry-run     # show what would be destroyed (default)
#   ./teardown.sh --yes-destroy # actually do it
# ---------------------------------------------------------------------------
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/hosts.env"

MODE=dry
case "${1:-}" in
  --yes-destroy) MODE=go ;;
  --dry-run|"")  MODE=dry ;;
  -h|--help)     sed -n '2,36p' "$0"; exit 0 ;;
  *) echo "unknown argument: $1" >&2; exit 2 ;;
esac

R="--region $AUDIT_REGION"

echo "=============================================================================="
echo "pgrust RC1 audit environment teardown   [mode: $MODE]"
echo "=============================================================================="
echo
echo "Instances to terminate:"
for i in "$CB_INSTANCE_ID" "$OLTP_SERVER_INSTANCE_ID" "$OLTP_CLIENT_INSTANCE_ID"; do
  # shellcheck disable=SC2086
  aws ec2 describe-instances $R --instance-ids "$i" \
      --query 'Reservations[].Instances[].[InstanceId,InstanceType,State.Name,Tags[?Key==`Name`].Value|[0]]' \
      --output text 2>/dev/null | sed 's/^/  /' || echo "  $i  (not found — already gone?)"
done
echo
echo "Elastic IPs to release:"
for a in "$AUDIT_EIP_CB" "$AUDIT_EIP_OLTP_SERVER" "$AUDIT_EIP_OLTP_CLIENT"; do
  # shellcheck disable=SC2086
  aws ec2 describe-addresses $R --allocation-ids "$a" \
      --query 'Addresses[].[AllocationId,PublicIp,InstanceId]' --output text 2>/dev/null \
      | sed 's/^/  /' || echo "  $a  (not found)"
done
echo
echo "Volumes that OUTLIVE their instance (DeleteOnTermination=false):"
# shellcheck disable=SC2086
aws ec2 describe-volumes $R --volume-ids "${CB_REF_VOLUME:-}" \
    --query 'Volumes[].[VolumeId,VolumeType,Size,State]' --output text 2>/dev/null \
    | sed 's/^/  /' || echo "  (none found)"
echo "  ^ this one holds the loaded ClickBench reference dataset (~\$50/mo)."
echo "    Terminating the instance does NOT delete it. This script does."
echo
echo "Security group : $AUDIT_SG"
echo "Key pair       : $AUDIT_KEYPAIR"
echo
echo "IRRECOVERABLE: the oltp-server instance store holds all four OLTP"
echo "datadirs (~800 GB, many hours of load time). Terminating destroys them."
echo

if [ "$MODE" = dry ]; then
  echo "Dry run. Nothing was changed."
  echo "Re-run with --yes-destroy to actually tear down."
  exit 0
fi

read -r -p "Type DESTROY to confirm: " ans
[ "$ans" = "DESTROY" ] || { echo "Aborted."; exit 1; }

echo
echo "terminating instances ..."
# shellcheck disable=SC2086
aws ec2 terminate-instances $R --instance-ids \
    "$CB_INSTANCE_ID" "$OLTP_SERVER_INSTANCE_ID" "$OLTP_CLIENT_INSTANCE_ID" \
    --query 'TerminatingInstances[].[InstanceId,CurrentState.Name]' --output text

echo "waiting for termination (EIPs cannot be released while associated) ..."
# shellcheck disable=SC2086
aws ec2 wait instance-terminated $R --instance-ids \
    "$CB_INSTANCE_ID" "$OLTP_SERVER_INSTANCE_ID" "$OLTP_CLIENT_INSTANCE_ID"

if [ -n "${CB_REF_VOLUME:-}" ]; then
  echo "deleting the reference data volume ..."
  # shellcheck disable=SC2086
  aws ec2 wait volume-available $R --volume-ids "$CB_REF_VOLUME" 2>/dev/null
  # shellcheck disable=SC2086
  aws ec2 delete-volume $R --volume-id "$CB_REF_VOLUME" \
    && echo "  deleted $CB_REF_VOLUME" \
    || echo "  could not delete $CB_REF_VOLUME — DELETE IT BY HAND or it bills forever"
fi

echo "releasing Elastic IPs ..."
for a in "$AUDIT_EIP_CB" "$AUDIT_EIP_OLTP_SERVER" "$AUDIT_EIP_OLTP_CLIENT"; do
  # shellcheck disable=SC2086
  aws ec2 release-address $R --allocation-id "$a" && echo "  released $a" \
    || echo "  could not release $a (already released?)"
done

echo "deleting security group ..."
# shellcheck disable=SC2086
aws ec2 delete-security-group $R --group-id "$AUDIT_SG" && echo "  deleted $AUDIT_SG" \
  || echo "  could not delete $AUDIT_SG (still in use? retry in a minute)"

echo "deleting key pair ..."
# shellcheck disable=SC2086
aws ec2 delete-key-pair $R --key-name "$AUDIT_KEYPAIR" && echo "  deleted $AUDIT_KEYPAIR"

echo
echo "Teardown complete. Verify nothing is left:"
echo "  aws ec2 describe-instances $R --filters Name=tag:lane,Values=audit \\"
echo "      Name=instance-state-name,Values=running,stopped \\"
echo "      --query 'Reservations[].Instances[].InstanceId'"
echo "  aws ec2 describe-addresses $R --filters Name=tag:lane,Values=audit"
echo "=============================================================================="
