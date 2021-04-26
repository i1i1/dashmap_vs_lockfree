use async_std::{sync::Mutex, task};
use criterion::async_executor::AsyncStdExecutor;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use dashmap::DashMap;
use lockfree::map::Map;
use rand::distributions::Standard;
use rand::prelude::*;

use std::{collections::HashMap, hash::Hash, sync::Arc};

#[async_trait::async_trait]
trait MapTrait: Send {
    type K;
    type V;

    fn new() -> Self;
    async fn insert(&self, k: Self::K, v: Self::V);
}

#[async_trait::async_trait]
impl<K: Hash + Ord + Send + Sync, V: Send + Sync> MapTrait for Map<K, V> {
    type K = K;
    type V = V;
    fn new() -> Self {
        Self::new()
    }
    async fn insert(&self, k: K, v: V) {
        self.insert(k, v);
    }
}

#[async_trait::async_trait]
impl<K: Hash + Eq + Send, V: Send> MapTrait for Mutex<HashMap<K, V>> {
    type K = K;
    type V = V;
    fn new() -> Self {
        Mutex::new(HashMap::new())
    }
    async fn insert(&self, k: K, v: V) {
        self.lock().await.insert(k, v);
    }
}

#[async_trait::async_trait]
impl<K: Hash + Eq + Send + Sync, V: Send + Sync> MapTrait for DashMap<K, V> {
    type K = K;
    type V = V;
    fn new() -> Self {
        Self::new()
    }
    async fn insert(&self, k: K, v: V) {
        self.insert(k, v);
    }
}

async fn spin_inserts<M>(threads: usize)
where
    M: MapTrait + Sync + 'static,
    Standard: Distribution<M::K> + Distribution<M::V>,
    M::V: Send,
    M::K: Send,
{
    let map = Arc::new(M::new());
    let handles = (0..threads)
        .map(|_| {
            let map = Arc::clone(&map);
            task::spawn(async move {
                for _ in 0..50_000 {
                    map.insert(rand::random(), rand::random()).await;
                }
            })
        })
        .collect::<Vec<_>>();
    for h in handles {
        h.await
    }
}

fn insert(c: &mut Criterion) {
    for i in 1..=8 {
        c.bench_with_input(BenchmarkId::new("lockfree insert", i), &(), |b, _| {
            b.to_async(AsyncStdExecutor)
                .iter(|| spin_inserts::<Map<u64, u64>>(i))
        });
        c.bench_with_input(BenchmarkId::new("dashmap insert", i), &(), |b, _| {
            b.to_async(AsyncStdExecutor)
                .iter(|| spin_inserts::<DashMap<u64, u64>>(i))
        });
        c.bench_with_input(BenchmarkId::new("mutexmap insert", i), &(), |b, _| {
            b.to_async(AsyncStdExecutor)
                .iter(|| spin_inserts::<Mutex<HashMap<u64, u64>>>(i))
        });
    }
}

criterion_group!(benches, insert);
criterion_main!(benches);
