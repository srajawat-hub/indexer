use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(i32)]
pub enum OrderStatus {
    Pending = 0,
    Verified = 1,
    Failed = 2,
    TimedOut = 3,
}

impl OrderStatus {
    pub fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(OrderStatus::Pending),
            1 => Some(OrderStatus::Verified),
            2 => Some(OrderStatus::Failed),
            3 => Some(OrderStatus::TimedOut),
            _ => None,
        }
    }

    pub fn to_i32(self) -> i32 {
        self as i32
    }
}

impl fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status_str = match self {
            OrderStatus::Pending => "pending",
            OrderStatus::Verified => "verified",
            OrderStatus::Failed => "failed",
            OrderStatus::TimedOut => "timed_out",
        };
        write!(f, "{}", status_str)
    }
}

impl From<OrderStatus> for i32 {
    fn from(status: OrderStatus) -> Self {
        status.to_i32()
    }
}

impl TryFrom<i32> for OrderStatus {
    type Error = String;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        OrderStatus::from_i32(value).ok_or_else(|| format!("Invalid order status: {}", value))
    }
} 