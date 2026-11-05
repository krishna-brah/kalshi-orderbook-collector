//! An aggregated price-level order book.

use std::collections::BTreeMap;

use crate::Side;

/// How one price level changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LevelChange {
    /// Add this signed amount to the level (Kalshi `orderbook_delta`).
    Delta(i64),
    /// Replace the level's size with this amount (Polymarket `price_change`).
    Absolute(i64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookError {
    /// A delta took a level below zero: this copy of the book has missed data.
    NegativeQuantity { side: Side, price: i64, quantity: i64 },
}

impl std::fmt::Display for BookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NegativeQuantity { side, price, quantity } => {
                write!(f, "{side} level {price} would go to {quantity}")
            }
        }
    }
}

impl std::error::Error for BookError {}

/// Bids and asks keyed by fixed-point price. A level is present only while its
/// quantity is positive.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Book {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

impl Book {
    /// Builds a book from snapshot levels. Levels with a non-positive size are
    /// skipped; a repeated price keeps its last size.
    pub fn from_levels(
        bids: impl IntoIterator<Item = (i64, i64)>,
        asks: impl IntoIterator<Item = (i64, i64)>,
    ) -> Self {
        let keep = |levels: &mut BTreeMap<i64, i64>, (price, qty): (i64, i64)| {
            if qty > 0 {
                levels.insert(price, qty);
            }
        };
        let mut book = Self::default();
        for level in bids {
            keep(&mut book.bids, level);
        }
        for level in asks {
            keep(&mut book.asks, level);
        }
        book
    }

    /// Applies one level change and returns the level's new absolute size
    /// (zero means the level is gone). On error the book is unchanged.
    pub fn apply(&mut self, side: Side, price: i64, change: LevelChange) -> Result<i64, BookError> {
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let current = levels.get(&price).copied().unwrap_or(0);
        let next = match change {
            LevelChange::Delta(delta) => current + delta,
            LevelChange::Absolute(quantity) => quantity,
        };
        if next < 0 {
            return Err(BookError::NegativeQuantity { side, price, quantity: next });
        }
        if next == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, next);
        }
        Ok(next)
    }

    pub fn quantity(&self, side: Side, price: i64) -> i64 {
        let levels = match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        };
        levels.get(&price).copied().unwrap_or(0)
    }

    /// Bids, best (highest) first.
    pub fn bids(&self) -> impl Iterator<Item = (i64, i64)> + '_ {
        self.bids.iter().rev().map(|(&p, &q)| (p, q))
    }

    /// Asks, best (lowest) first.
    pub fn asks(&self) -> impl Iterator<Item = (i64, i64)> + '_ {
        self.asks.iter().map(|(&p, &q)| (p, q))
    }

    pub fn best_bid(&self) -> Option<i64> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<i64> {
        self.asks.keys().next().copied()
    }

    pub fn is_crossed(&self) -> bool {
        matches!((self.best_bid(), self.best_ask()), (Some(bid), Some(ask)) if bid >= ask)
    }

    pub fn level_count(&self) -> usize {
        self.bids.len() + self.asks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deltas_accumulate_and_zero_removes_the_level() {
        let mut book = Book::default();
        assert_eq!(book.apply(Side::Buy, 400_000, LevelChange::Delta(5_000_000)), Ok(5_000_000));
        assert_eq!(book.apply(Side::Buy, 400_000, LevelChange::Delta(-2_000_000)), Ok(3_000_000));
        assert_eq!(book.quantity(Side::Buy, 400_000), 3_000_000);
        assert_eq!(book.apply(Side::Buy, 400_000, LevelChange::Delta(-3_000_000)), Ok(0));
        assert!(book.is_empty(), "a level at zero must be deleted, not kept at 0");
    }

    #[test]
    fn a_delta_below_zero_is_refused_and_leaves_the_book_alone() {
        let mut book = Book::from_levels([(400_000, 1_000_000)], []);
        let before = book.clone();
        assert_eq!(
            book.apply(Side::Buy, 400_000, LevelChange::Delta(-2_000_000)),
            Err(BookError::NegativeQuantity { side: Side::Buy, price: 400_000, quantity: -1_000_000 })
        );
        assert_eq!(book, before);
    }

    #[test]
    fn absolute_changes_replace_the_level() {
        let mut book = Book::from_levels([], [(600_000, 7_000_000)]);
        assert_eq!(book.apply(Side::Sell, 600_000, LevelChange::Absolute(2_000_000)), Ok(2_000_000));
        assert_eq!(book.apply(Side::Sell, 600_000, LevelChange::Absolute(0)), Ok(0));
        assert!(book.is_empty());
    }

    #[test]
    fn levels_come_out_best_first() {
        let book = Book::from_levels(
            [(300_000, 1), (450_000, 2), (100_000, 3)],
            [(900_000, 4), (550_000, 5), (700_000, 6)],
        );
        assert_eq!(book.bids().collect::<Vec<_>>(), [(450_000, 2), (300_000, 1), (100_000, 3)]);
        assert_eq!(book.asks().collect::<Vec<_>>(), [(550_000, 5), (700_000, 6), (900_000, 4)]);
        assert!(!book.is_crossed());
        assert!(Book::from_levels([(600_000, 1)], [(550_000, 1)]).is_crossed());
    }

    #[test]
    fn snapshot_levels_with_no_size_are_dropped() {
        let book = Book::from_levels([(100_000, 0), (200_000, 1)], [(800_000, -1)]);
        assert_eq!(book.level_count(), 1);
    }
}
