//! 라우팅 정책
//!
//! 서버 선택 전략을 정의합니다.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::RwLock;

use super::super::driver::ServerAddress;

/// 라우팅 정책
#[derive(Debug, Clone, Copy, Default)]
pub enum RoutingPolicy {
    /// 라운드 로빈 (기본값)
    #[default]
    RoundRobin,
    /// 최소 연결
    LeastConnections,
    /// 랜덤
    Random,
}

/// 서버별 연결 카운터
#[derive(Debug, Default)]
pub struct ConnectionCounter {
    /// 서버별 활성 연결 수
    counts: RwLock<HashMap<ServerAddress, usize>>,
}

impl ConnectionCounter {
    /// 새 카운터 생성
    pub fn new() -> Self {
        Self {
            counts: RwLock::new(HashMap::new()),
        }
    }

    /// 연결 획득 시 카운트 증가
    pub fn acquire(&self, server: &ServerAddress) {
        let mut counts = self.counts.write();
        *counts.entry(server.clone()).or_insert(0) += 1;
    }

    /// 연결 해제 시 카운트 감소
    pub fn release(&self, server: &ServerAddress) {
        let mut counts = self.counts.write();
        if let Some(count) = counts.get_mut(server) {
            *count = count.saturating_sub(1);
        }
    }

    /// 특정 서버의 연결 수 조회
    pub fn get(&self, server: &ServerAddress) -> usize {
        self.counts.read().get(server).copied().unwrap_or(0)
    }

    /// 최소 연결 서버 선택
    pub fn select_least<'a>(&self, servers: &'a [ServerAddress]) -> Option<&'a ServerAddress> {
        if servers.is_empty() {
            return None;
        }

        let counts = self.counts.read();
        servers
            .iter()
            .min_by_key(|s| counts.get(*s).copied().unwrap_or(0))
    }

    /// 모든 카운트 리셋
    pub fn reset(&self) {
        self.counts.write().clear();
    }

    /// 현재 연결 수 스냅샷
    pub fn snapshot(&self) -> HashMap<ServerAddress, usize> {
        self.counts.read().clone()
    }
}

/// 서버 선택기
pub struct ServerSelector {
    /// 라우팅 정책
    policy: RoutingPolicy,
    /// 라운드 로빈 인덱스
    round_robin_index: AtomicUsize,
    /// 연결 카운터 (LeastConnections용)
    connection_counter: ConnectionCounter,
}

impl ServerSelector {
    /// 새 선택기 생성
    pub fn new(policy: RoutingPolicy) -> Self {
        Self {
            policy,
            round_robin_index: AtomicUsize::new(0),
            connection_counter: ConnectionCounter::new(),
        }
    }

    /// 기본 선택기 (라운드 로빈)
    pub fn round_robin() -> Self {
        Self::new(RoutingPolicy::RoundRobin)
    }

    /// 최소 연결 선택기
    pub fn least_connections() -> Self {
        Self::new(RoutingPolicy::LeastConnections)
    }

    /// 랜덤 선택기
    pub fn random() -> Self {
        Self::new(RoutingPolicy::Random)
    }

    /// 서버 목록에서 하나 선택
    pub fn select<'a>(&self, servers: &'a [ServerAddress]) -> Option<&'a ServerAddress> {
        if servers.is_empty() {
            return None;
        }

        match self.policy {
            RoutingPolicy::RoundRobin => {
                let index = self.round_robin_index.fetch_add(1, Ordering::Relaxed);
                Some(&servers[index % servers.len()])
            }
            RoutingPolicy::Random => {
                use rand::Rng;
                let index = rand::thread_rng().gen_range(0..servers.len());
                Some(&servers[index])
            }
            RoutingPolicy::LeastConnections => {
                // 최소 연결 서버 선택
                self.connection_counter.select_least(servers)
            }
        }
    }

    /// 특정 서버를 제외하고 선택
    pub fn select_except<'a>(
        &self,
        servers: &'a [ServerAddress],
        except: &ServerAddress,
    ) -> Option<&'a ServerAddress> {
        let filtered: Vec<&ServerAddress> = servers.iter().filter(|s| *s != except).collect();

        if filtered.is_empty() {
            return None;
        }

        match self.policy {
            RoutingPolicy::RoundRobin => {
                let index = self.round_robin_index.fetch_add(1, Ordering::Relaxed);
                Some(filtered[index % filtered.len()])
            }
            RoutingPolicy::Random => {
                use rand::Rng;
                let index = rand::thread_rng().gen_range(0..filtered.len());
                Some(filtered[index])
            }
            RoutingPolicy::LeastConnections => {
                // filtered는 참조 슬라이스이므로 별도 처리
                let counts = self.connection_counter.snapshot();
                filtered
                    .into_iter()
                    .min_by_key(|s| counts.get(*s).copied().unwrap_or(0))
            }
        }
    }

    /// 연결 획득 알림 (LeastConnections용)
    pub fn on_connection_acquired(&self, server: &ServerAddress) {
        self.connection_counter.acquire(server);
    }

    /// 연결 해제 알림 (LeastConnections용)
    pub fn on_connection_released(&self, server: &ServerAddress) {
        self.connection_counter.release(server);
    }

    /// 특정 서버의 연결 수 조회
    pub fn connection_count(&self, server: &ServerAddress) -> usize {
        self.connection_counter.get(server)
    }

    /// 인덱스 리셋
    pub fn reset(&self) {
        self.round_robin_index.store(0, Ordering::Relaxed);
        self.connection_counter.reset();
    }

    /// 현재 정책 조회
    pub fn policy(&self) -> RoutingPolicy {
        self.policy
    }
}

impl Default for ServerSelector {
    fn default() -> Self {
        Self::round_robin()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_robin_selection() {
        let selector = ServerSelector::round_robin();
        let servers = vec![
            ServerAddress::new("server1", 7687),
            ServerAddress::new("server2", 7687),
            ServerAddress::new("server3", 7687),
        ];

        // 라운드 로빈으로 순환 선택
        let s1 = selector.select(&servers).unwrap();
        let s2 = selector.select(&servers).unwrap();
        let s3 = selector.select(&servers).unwrap();
        let s4 = selector.select(&servers).unwrap();

        assert_eq!(s1.host, "server1");
        assert_eq!(s2.host, "server2");
        assert_eq!(s3.host, "server3");
        assert_eq!(s4.host, "server1"); // 다시 처음으로
    }

    #[test]
    fn test_empty_servers() {
        let selector = ServerSelector::round_robin();
        let servers: Vec<ServerAddress> = vec![];

        assert!(selector.select(&servers).is_none());
    }

    #[test]
    fn test_select_except() {
        let selector = ServerSelector::new(RoutingPolicy::RoundRobin);
        let servers = vec![
            ServerAddress::new("server1", 7687),
            ServerAddress::new("server2", 7687),
        ];

        let except = ServerAddress::new("server1", 7687);
        let selected = selector.select_except(&servers, &except);

        assert!(selected.is_some());
        assert_eq!(selected.unwrap().host, "server2");
    }

    #[test]
    fn test_select_except_all_excluded() {
        let selector = ServerSelector::round_robin();
        let servers = vec![ServerAddress::new("server1", 7687)];

        let except = ServerAddress::new("server1", 7687);
        let selected = selector.select_except(&servers, &except);

        assert!(selected.is_none());
    }

    #[test]
    fn test_reset() {
        let selector = ServerSelector::round_robin();
        let servers = vec![
            ServerAddress::new("server1", 7687),
            ServerAddress::new("server2", 7687),
        ];

        selector.select(&servers);
        selector.select(&servers);
        selector.reset();

        let s = selector.select(&servers).unwrap();
        assert_eq!(s.host, "server1");
    }

    #[test]
    fn test_random_selection() {
        let selector = ServerSelector::new(RoutingPolicy::Random);
        let servers = vec![
            ServerAddress::new("server1", 7687),
            ServerAddress::new("server2", 7687),
            ServerAddress::new("server3", 7687),
        ];

        // 랜덤 선택이 유효한 서버를 반환하는지 확인
        for _ in 0..10 {
            let selected = selector.select(&servers);
            assert!(selected.is_some());
            assert!(servers.contains(selected.unwrap()));
        }
    }

    #[test]
    fn test_connection_counter() {
        let counter = ConnectionCounter::new();
        let server1 = ServerAddress::new("server1", 7687);
        let server2 = ServerAddress::new("server2", 7687);

        // 초기값은 0
        assert_eq!(counter.get(&server1), 0);
        assert_eq!(counter.get(&server2), 0);

        // 연결 획득
        counter.acquire(&server1);
        counter.acquire(&server1);
        counter.acquire(&server2);

        assert_eq!(counter.get(&server1), 2);
        assert_eq!(counter.get(&server2), 1);

        // 연결 해제
        counter.release(&server1);
        assert_eq!(counter.get(&server1), 1);

        // underflow 방지
        counter.release(&server2);
        counter.release(&server2);
        assert_eq!(counter.get(&server2), 0);

        // 리셋
        counter.reset();
        assert_eq!(counter.get(&server1), 0);
    }

    #[test]
    fn test_connection_counter_select_least() {
        let counter = ConnectionCounter::new();
        let servers = vec![
            ServerAddress::new("server1", 7687),
            ServerAddress::new("server2", 7687),
            ServerAddress::new("server3", 7687),
        ];

        // 모두 0일 때 첫 번째 선택
        let selected = counter.select_least(&servers).unwrap();
        assert!(servers.contains(selected));

        // server1에 연결 추가
        counter.acquire(&servers[0]);
        counter.acquire(&servers[0]);
        // server2에 연결 추가
        counter.acquire(&servers[1]);

        // server3이 최소 (0개)
        let selected = counter.select_least(&servers).unwrap();
        assert_eq!(selected.host, "server3");
    }

    #[test]
    fn test_least_connections_selection() {
        let selector = ServerSelector::least_connections();
        let servers = vec![
            ServerAddress::new("server1", 7687),
            ServerAddress::new("server2", 7687),
            ServerAddress::new("server3", 7687),
        ];

        // server1에 2개, server2에 1개 연결 시뮬레이션
        selector.on_connection_acquired(&servers[0]);
        selector.on_connection_acquired(&servers[0]);
        selector.on_connection_acquired(&servers[1]);

        // server3이 선택되어야 함 (0개 연결)
        let selected = selector.select(&servers).unwrap();
        assert_eq!(selected.host, "server3");

        // server3에 연결 추가
        selector.on_connection_acquired(&servers[2]);
        selector.on_connection_acquired(&servers[2]);
        selector.on_connection_acquired(&servers[2]);

        // 이제 server2가 최소 (1개)
        let selected = selector.select(&servers).unwrap();
        assert_eq!(selected.host, "server2");

        // 연결 해제 후 재선택
        selector.on_connection_released(&servers[0]);
        selector.on_connection_released(&servers[0]);

        // server1이 최소 (0개)
        let selected = selector.select(&servers).unwrap();
        assert_eq!(selected.host, "server1");
    }

    #[test]
    fn test_least_connections_select_except() {
        let selector = ServerSelector::least_connections();
        let servers = vec![
            ServerAddress::new("server1", 7687),
            ServerAddress::new("server2", 7687),
            ServerAddress::new("server3", 7687),
        ];

        // server2에 연결 추가
        selector.on_connection_acquired(&servers[1]);

        // server1 제외하고 선택 - server3이 선택되어야 함 (0개)
        let except = &servers[0];
        let selected = selector.select_except(&servers, except).unwrap();
        assert_eq!(selected.host, "server3");
    }

    #[test]
    fn test_connection_count_tracking() {
        let selector = ServerSelector::least_connections();
        let server = ServerAddress::new("server1", 7687);

        assert_eq!(selector.connection_count(&server), 0);

        selector.on_connection_acquired(&server);
        selector.on_connection_acquired(&server);
        assert_eq!(selector.connection_count(&server), 2);

        selector.on_connection_released(&server);
        assert_eq!(selector.connection_count(&server), 1);
    }

    #[test]
    fn test_selector_policy() {
        let selector1 = ServerSelector::round_robin();
        assert!(matches!(selector1.policy(), RoutingPolicy::RoundRobin));

        let selector2 = ServerSelector::least_connections();
        assert!(matches!(selector2.policy(), RoutingPolicy::LeastConnections));

        let selector3 = ServerSelector::random();
        assert!(matches!(selector3.policy(), RoutingPolicy::Random));
    }
}
