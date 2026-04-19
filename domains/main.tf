terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket = "pagerfree-terraform-state"
    key    = "pgrust/domains/terraform.tfstate"
    region = "us-west-2"
  }
}

# CloudFront + ACM must live in us-east-1.
provider "aws" {
  region = "us-east-1"
}

# S3 bucket is in us-west-2; aliased provider for its bucket policy.
provider "aws" {
  alias  = "usw2"
  region = "us-west-2"
}

locals {
  apex        = "pgrust.com"
  www         = "www.pgrust.com"
  bucket_name = "pgrust"
}

# Hosted zone was auto-created by Route 53 Registrar at domain registration.
# Imported via: terraform import aws_route53_zone.pgrust_com Z0795405IN00H8WFZEZA
resource "aws_route53_zone" "pgrust_com" {
  name          = local.apex
  comment       = "HostedZone created by Route53 Registrar"
  force_destroy = false
}

# ACM cert covering apex + www, DNS validation via the hosted zone.
resource "aws_acm_certificate" "pgrust" {
  domain_name               = local.apex
  subject_alternative_names = [local.www]
  validation_method         = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_route53_record" "acm_validation" {
  # Static keys so terraform can plan/import without waiting on the cert resource.
  for_each = toset([local.apex, local.www])

  zone_id = aws_route53_zone.pgrust_com.zone_id
  name    = one([for dvo in aws_acm_certificate.pgrust.domain_validation_options : dvo.resource_record_name if dvo.domain_name == each.value])
  type    = one([for dvo in aws_acm_certificate.pgrust.domain_validation_options : dvo.resource_record_type if dvo.domain_name == each.value])
  records = [one([for dvo in aws_acm_certificate.pgrust.domain_validation_options : dvo.resource_record_value if dvo.domain_name == each.value])]
  ttl     = 60

  allow_overwrite = true
}

resource "aws_acm_certificate_validation" "pgrust" {
  certificate_arn         = aws_acm_certificate.pgrust.arn
  validation_record_fqdns = [for r in aws_route53_record.acm_validation : r.fqdn]
}

# Origin Access Control — modern replacement for OAI. Lets the distribution
# read from the private S3 bucket via SigV4.
resource "aws_cloudfront_origin_access_control" "pgrust" {
  name                              = "pgrust-s3-oac"
  description                       = "OAC for pgrust.com static site"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

# Viewer-request function: redirect www.pgrust.com → pgrust.com (apex).
resource "aws_cloudfront_function" "www_to_apex" {
  name    = "pgrust-www-to-apex"
  runtime = "cloudfront-js-2.0"
  comment = "301 redirect www.pgrust.com to apex"
  publish = true
  code    = <<-EOT
    function handler(event) {
      var host = event.request.headers.host.value;
      if (host === 'www.pgrust.com') {
        return {
          statusCode: 301,
          statusDescription: 'Moved Permanently',
          headers: {
            location: { value: 'https://pgrust.com' + event.request.uri }
          }
        };
      }
      return event.request;
    }
  EOT
}

resource "aws_cloudfront_distribution" "pgrust" {
  enabled             = true
  is_ipv6_enabled     = true
  default_root_object = "index.html"
  price_class         = "PriceClass_100"
  comment             = "pgrust.com static site"

  aliases = [local.apex, local.www]

  origin {
    domain_name              = "${local.bucket_name}.s3.us-west-2.amazonaws.com"
    origin_id                = "s3-pgrust"
    origin_access_control_id = aws_cloudfront_origin_access_control.pgrust.id
  }

  default_cache_behavior {
    target_origin_id       = "s3-pgrust"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    compress               = true

    # CachingOptimized managed cache policy (AWS-managed).
    cache_policy_id = "658327ea-f89d-4fab-a63d-7e88639e58f6"

    function_association {
      event_type   = "viewer-request"
      function_arn = aws_cloudfront_function.www_to_apex.arn
    }
  }

  # Simple SPA-style fallback: 403/404 from S3 → serve index.html with 200.
  custom_error_response {
    error_code            = 403
    response_code         = 200
    response_page_path    = "/index.html"
    error_caching_min_ttl = 10
  }

  custom_error_response {
    error_code            = 404
    response_code         = 200
    response_page_path    = "/index.html"
    error_caching_min_ttl = 10
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    acm_certificate_arn      = aws_acm_certificate_validation.pgrust.certificate_arn
    ssl_support_method       = "sni-only"
    minimum_protocol_version = "TLSv1.2_2021"
  }
}

# Grant the distribution permission to read objects from the bucket.
resource "aws_s3_bucket_policy" "pgrust" {
  provider = aws.usw2
  bucket   = local.bucket_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowCloudFrontServicePrincipal"
        Effect    = "Allow"
        Principal = { Service = "cloudfront.amazonaws.com" }
        Action    = "s3:GetObject"
        Resource  = "arn:aws:s3:::${local.bucket_name}/*"
        Condition = {
          StringEquals = {
            "AWS:SourceArn" = aws_cloudfront_distribution.pgrust.arn
          }
        }
      }
    ]
  })
}

resource "aws_route53_record" "apex_a" {
  zone_id = aws_route53_zone.pgrust_com.zone_id
  name    = local.apex
  type    = "A"

  alias {
    name                   = aws_cloudfront_distribution.pgrust.domain_name
    zone_id                = aws_cloudfront_distribution.pgrust.hosted_zone_id
    evaluate_target_health = false
  }
}

resource "aws_route53_record" "apex_aaaa" {
  zone_id = aws_route53_zone.pgrust_com.zone_id
  name    = local.apex
  type    = "AAAA"

  alias {
    name                   = aws_cloudfront_distribution.pgrust.domain_name
    zone_id                = aws_cloudfront_distribution.pgrust.hosted_zone_id
    evaluate_target_health = false
  }
}

resource "aws_route53_record" "www_a" {
  zone_id = aws_route53_zone.pgrust_com.zone_id
  name    = local.www
  type    = "A"

  alias {
    name                   = aws_cloudfront_distribution.pgrust.domain_name
    zone_id                = aws_cloudfront_distribution.pgrust.hosted_zone_id
    evaluate_target_health = false
  }
}

resource "aws_route53_record" "www_aaaa" {
  zone_id = aws_route53_zone.pgrust_com.zone_id
  name    = local.www
  type    = "AAAA"

  alias {
    name                   = aws_cloudfront_distribution.pgrust.domain_name
    zone_id                = aws_cloudfront_distribution.pgrust.hosted_zone_id
    evaluate_target_health = false
  }
}

output "nameservers" {
  value       = aws_route53_zone.pgrust_com.name_servers
  description = "Registrar NS records (already pointing here via Route53 Registrar default)."
}

output "cloudfront_distribution_id" {
  value       = aws_cloudfront_distribution.pgrust.id
  description = "Distribution ID — consumed by web/wasm-demo/deploy.sh for cache invalidation."
}

output "cloudfront_domain_name" {
  value       = aws_cloudfront_distribution.pgrust.domain_name
  description = "CloudFront-assigned domain (for debugging; users should use pgrust.com)."
}
