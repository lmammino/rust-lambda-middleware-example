//! Extract the client IP from an incoming Lambda HTTP request.
//!
//! Priority order:
//!   1. `X-Forwarded-For` (first valid IP in the list)
//!   2. `X-Real-IP`
//!   3. `CF-Connecting-IP` (Cloudflare)
//!
//! Adapted from https://github.com/lmammino/geo-redirect-lambda.
//!
//! Trust caveat: these headers are only trustworthy if you control every hop
//! between the client and the Lambda. Behind API Gateway or an ALB they are
//! set by the proxy and safe. If you expose your function URL directly without
//! a trusted proxy, any caller can forge them.

use std::net::IpAddr;
use std::str::FromStr;

use lambda_http::Request;

pub fn extract_ip(request: &Request) -> Option<IpAddr> {
    let headers = request.headers();

    if let Some(value) = headers.get("x-forwarded-for") {
        if let Ok(s) = value.to_str() {
            if let Some(ip) = parse_forwarded_for(s) {
                return Some(ip);
            }
        }
    }

    if let Some(value) = headers.get("x-real-ip") {
        if let Ok(s) = value.to_str() {
            if let Ok(ip) = IpAddr::from_str(s.trim()) {
                return Some(ip);
            }
        }
    }

    if let Some(value) = headers.get("cf-connecting-ip") {
        if let Ok(s) = value.to_str() {
            if let Ok(ip) = IpAddr::from_str(s.trim()) {
                return Some(ip);
            }
        }
    }

    None
}

fn parse_forwarded_for(header: &str) -> Option<IpAddr> {
    header
        .split(',')
        .map(str::trim)
        .find_map(|candidate| IpAddr::from_str(candidate).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lambda_http::{Body, Request};

    fn req() -> Request {
        Request::new(Body::Empty)
    }

    #[test]
    fn x_forwarded_for_single_ip() {
        let mut r = req();
        r.headers_mut()
            .insert("x-forwarded-for", "192.168.1.1".parse().unwrap());
        assert_eq!(
            extract_ip(&r),
            Some(IpAddr::from_str("192.168.1.1").unwrap())
        );
    }

    #[test]
    fn x_forwarded_for_picks_first_valid() {
        let mut r = req();
        r.headers_mut().insert(
            "x-forwarded-for",
            "invalid, 203.0.113.1, 10.0.0.1".parse().unwrap(),
        );
        assert_eq!(
            extract_ip(&r),
            Some(IpAddr::from_str("203.0.113.1").unwrap())
        );
    }

    #[test]
    fn fallback_to_x_real_ip() {
        let mut r = req();
        r.headers_mut()
            .insert("x-real-ip", "203.0.113.5".parse().unwrap());
        assert_eq!(
            extract_ip(&r),
            Some(IpAddr::from_str("203.0.113.5").unwrap())
        );
    }

    #[test]
    fn fallback_to_cf_connecting_ip() {
        let mut r = req();
        r.headers_mut()
            .insert("cf-connecting-ip", "198.51.100.7".parse().unwrap());
        assert_eq!(
            extract_ip(&r),
            Some(IpAddr::from_str("198.51.100.7").unwrap())
        );
    }

    #[test]
    fn priority_forwarded_for_wins() {
        let mut r = req();
        r.headers_mut()
            .insert("cf-connecting-ip", "198.51.100.7".parse().unwrap());
        r.headers_mut()
            .insert("x-real-ip", "203.0.113.5".parse().unwrap());
        r.headers_mut()
            .insert("x-forwarded-for", "192.168.1.1".parse().unwrap());
        assert_eq!(
            extract_ip(&r),
            Some(IpAddr::from_str("192.168.1.1").unwrap())
        );
    }

    #[test]
    fn ipv6_is_supported() {
        let mut r = req();
        r.headers_mut()
            .insert("x-forwarded-for", "2001:db8::1".parse().unwrap());
        assert_eq!(
            extract_ip(&r),
            Some(IpAddr::from_str("2001:db8::1").unwrap())
        );
    }

    #[test]
    fn none_when_no_headers() {
        assert_eq!(extract_ip(&req()), None);
    }
}
