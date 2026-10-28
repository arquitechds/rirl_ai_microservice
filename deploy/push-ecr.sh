#!/usr/bin/env bash
set -euo pipefail
: "${AWS_DEFAULT_REGION:?Set AWS_DEFAULT_REGION}"
: "${IMAGE_TAG:?Set an immutable IMAGE_TAG, e.g. union-dues-v2-001}"
ECR_REPOSITORY="${ECR_REPOSITORY:-rirl-union-dues}"
AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com"
# Create repository explicitly beforehand; permission/network errors must not masquerade as absence.
aws ecr describe-repositories --repository-names "$ECR_REPOSITORY" >/dev/null
aws ecr get-login-password --region "$AWS_DEFAULT_REGION" | docker login --username AWS --password-stdin "$ECR_REGISTRY"
docker buildx build --platform linux/amd64 --tag "$ECR_REGISTRY/$ECR_REPOSITORY:$IMAGE_TAG" --push "$(cd "$(dirname "$0")/.." && pwd)"
printf 'Image: %s\n' "$ECR_REGISTRY/$ECR_REPOSITORY:$IMAGE_TAG"
