//! 라우팅 테이블
//!
//! 클러스터의 서버 역할별 목록을 관리합니다.

use std::time::Instant;

use super::super::driver::ServerAddress;

/// 서버 역할
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ServerRole {
    /// 라우팅 테이블 제공자
    Route,
    /// 쓰기 트랜잭션 처리 (리더)
    Write,
    /// 읽기 트랜잭션 처리 (팔로워)
    Read,
}

impl ServerRole {
    /// 문자열에서 역할 파싱
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "ROUTE" => Some(Self::Route),
            "WRITE" => Some(Self::Write),
            "READ" => Some(Self::Read),
            _ => None,
        }
    }

    /// 역할을 문자열로 변환
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Route => "ROUTE",
            Self::Write => "WRITE",
            Self::Read => "READ",
        }
    }
}

/// 라우팅 테이블
#[derive(Debug, Clone)]
pub struct RoutingTable {
    /// 라우터 목록 (라우팅 테이블 조회용)
    pub routers: Vec<ServerAddress>,
    /// 라이터 목록 (쓰기 트랜잭션용)
    pub writers: Vec<ServerAddress>,
    /// 리더 목록 (읽기 트랜잭션용)
    pub readers: Vec<ServerAddress>,
    /// 데이터베이스 이름
    pub database: String,
    /// TTL (초)
    pub ttl_seconds: u64,
    /// 생성/갱신 시간
    pub updated_at: Instant,
}

impl RoutingTable {
    /// 새 라우팅 테이블 생성
    pub fn new(database: impl Into<String>) -> Self {
        Self {
            routers: Vec::new(),
            writers: Vec::new(),
            readers: Vec::new(),
            database: database.into(),
            ttl_seconds: 300, // 기본 5분
            updated_at: Instant::now(),
        }
    }

    /// 초기 라우터로 테이블 생성
    pub fn with_initial_routers(database: impl Into<String>, routers: Vec<ServerAddress>) -> Self {
        Self {
            routers: routers.clone(),
            writers: Vec::new(),
            readers: routers,
            database: database.into(),
            ttl_seconds: 0, // 즉시 갱신 필요
            updated_at: Instant::now(),
        }
    }

    /// 라우터 추가
    pub fn add_router(&mut self, address: ServerAddress) {
        if !self.routers.contains(&address) {
            self.routers.push(address);
        }
    }

    /// 라이터 추가
    pub fn add_writer(&mut self, address: ServerAddress) {
        if !self.writers.contains(&address) {
            self.writers.push(address);
        }
    }

    /// 리더 추가
    pub fn add_reader(&mut self, address: ServerAddress) {
        if !self.readers.contains(&address) {
            self.readers.push(address);
        }
    }

    /// 역할별 서버 추가
    pub fn add_server(&mut self, role: ServerRole, address: ServerAddress) {
        match role {
            ServerRole::Route => self.add_router(address),
            ServerRole::Write => self.add_writer(address),
            ServerRole::Read => self.add_reader(address),
        }
    }

    /// 라우팅 테이블이 만료되었는지 확인
    pub fn is_expired(&self) -> bool {
        self.updated_at.elapsed().as_secs() >= self.ttl_seconds
    }

    /// 갱신이 필요한지 확인 (TTL의 80% 경과)
    pub fn needs_refresh(&self) -> bool {
        let threshold = (self.ttl_seconds as f64 * 0.8) as u64;
        self.updated_at.elapsed().as_secs() >= threshold
    }

    /// 테이블 갱신 시간 업데이트
    pub fn mark_updated(&mut self) {
        self.updated_at = Instant::now();
    }

    /// TTL 설정
    pub fn set_ttl(&mut self, seconds: u64) {
        self.ttl_seconds = seconds;
    }

    /// 서버가 있는지 확인
    pub fn has_servers(&self) -> bool {
        !self.writers.is_empty() || !self.readers.is_empty()
    }

    /// 쓰기 가능한 서버가 있는지 확인
    pub fn has_writers(&self) -> bool {
        !self.writers.is_empty()
    }

    /// 읽기 가능한 서버가 있는지 확인
    pub fn has_readers(&self) -> bool {
        !self.readers.is_empty()
    }

    /// 라우터가 있는지 확인
    pub fn has_routers(&self) -> bool {
        !self.routers.is_empty()
    }

    /// 서버 제거
    pub fn remove_server(&mut self, address: &ServerAddress) {
        self.routers.retain(|a| a != address);
        self.writers.retain(|a| a != address);
        self.readers.retain(|a| a != address);
    }

    /// 테이블 초기화
    pub fn clear(&mut self) {
        self.routers.clear();
        self.writers.clear();
        self.readers.clear();
    }
}

impl Default for RoutingTable {
    fn default() -> Self {
        Self::new("zeta4g")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_role_from_str() {
        assert_eq!(ServerRole::from_str("ROUTE"), Some(ServerRole::Route));
        assert_eq!(ServerRole::from_str("route"), Some(ServerRole::Route));
        assert_eq!(ServerRole::from_str("WRITE"), Some(ServerRole::Write));
        assert_eq!(ServerRole::from_str("READ"), Some(ServerRole::Read));
        assert_eq!(ServerRole::from_str("UNKNOWN"), None);
    }

    #[test]
    fn test_routing_table_new() {
        let table = RoutingTable::new("mydb");
        assert_eq!(table.database, "mydb");
        assert!(table.routers.is_empty());
        assert!(table.writers.is_empty());
        assert!(table.readers.is_empty());
    }

    #[test]
    fn test_routing_table_add_servers() {
        let mut table = RoutingTable::new("testdb");

        table.add_router(ServerAddress::new("router1", 7687));
        table.add_writer(ServerAddress::new("writer1", 7687));
        table.add_reader(ServerAddress::new("reader1", 7687));
        table.add_reader(ServerAddress::new("reader2", 7687));

        assert_eq!(table.routers.len(), 1);
        assert_eq!(table.writers.len(), 1);
        assert_eq!(table.readers.len(), 2);
    }

    #[test]
    fn test_routing_table_no_duplicates() {
        let mut table = RoutingTable::new("testdb");
        let addr = ServerAddress::new("server1", 7687);

        table.add_router(addr.clone());
        table.add_router(addr.clone());

        assert_eq!(table.routers.len(), 1);
    }

    #[test]
    fn test_routing_table_has_servers() {
        let mut table = RoutingTable::new("testdb");
        assert!(!table.has_servers());
        assert!(!table.has_writers());
        assert!(!table.has_readers());

        table.add_writer(ServerAddress::new("writer1", 7687));
        assert!(table.has_servers());
        assert!(table.has_writers());
        assert!(!table.has_readers());
    }

    #[test]
    fn test_routing_table_remove_server() {
        let mut table = RoutingTable::new("testdb");
        let addr = ServerAddress::new("server1", 7687);

        table.add_router(addr.clone());
        table.add_writer(addr.clone());
        table.add_reader(addr.clone());

        table.remove_server(&addr);

        assert!(table.routers.is_empty());
        assert!(table.writers.is_empty());
        assert!(table.readers.is_empty());
    }

    #[test]
    fn test_routing_table_ttl() {
        let mut table = RoutingTable::new("testdb");
        table.set_ttl(60);

        // 새로 생성된 테이블은 만료되지 않음
        assert!(!table.is_expired());
        assert!(!table.needs_refresh());
    }

    #[test]
    fn test_routing_table_with_initial_routers() {
        let routers = vec![
            ServerAddress::new("router1", 7687),
            ServerAddress::new("router2", 7687),
        ];

        let table = RoutingTable::with_initial_routers("testdb", routers.clone());

        assert_eq!(table.routers.len(), 2);
        assert_eq!(table.readers.len(), 2); // 초기 라우터는 리더로도 사용
        assert!(table.writers.is_empty());
        assert_eq!(table.ttl_seconds, 0); // 즉시 갱신 필요
    }
}
