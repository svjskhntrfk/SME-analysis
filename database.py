import aiosqlite
from dataclasses import dataclass
from typing import List, Optional
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)    
@dataclass
class Company:
    name: str
    okved: str  # Полный ОКВЭД
    okved_1: str  # Первая часть
    okved_2: str  # Вторая часть
    okved_3: str  # Третья часть
    inn: str
    revenue: float
    growth_rate: float
    owner: str

class Database:
    def __init__(self, db_name: str = "companies.db"):
        self.db_name = db_name

    async def create_table(self):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS companies (
                    inn TEXT PRIMARY KEY,
                    name TEXT,
                    okved TEXT,
                    okved_1 TEXT,
                    okved_2 TEXT,
                    okved_3 TEXT,
                    revenue REAL,
                    growth_rate REAL,
                    owner TEXT
                )
            ''')
            logger.info("Таблица companies создана")
            await db.commit()

    async def company_exists(self, inn: str) -> bool:
        """Проверяет, существует ли компания с данным ИНН в базе"""
        async with aiosqlite.connect(self.db_name) as db:
            async with db.execute('SELECT 1 FROM companies WHERE inn = ?', (inn,)) as cursor:
                return await cursor.fetchone() is not None

    async def save_companies(self, companies: List[Company]):
        async with aiosqlite.connect(self.db_name) as db:
            for company in companies:
                if not await self.company_exists(company.inn):
                    await db.execute('''
                        INSERT INTO companies (
                            inn, name, okved, okved_1, okved_2, okved_3,
                            revenue, growth_rate, owner
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        company.inn, company.name, company.okved,
                        company.okved_1, company.okved_2, company.okved_3,
                        company.revenue, company.growth_rate, company.owner
                    ))
            await db.commit()

    async def get_company_by_inn(self, inn: str) -> Optional[Company]:
        async with aiosqlite.connect(self.db_name) as db:
            async with db.execute(
                'SELECT * FROM companies WHERE inn = ?', 
                (inn,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return Company(
                        inn=row[0],
                        name=row[1],
                        okved=row[2],
                        okved_1=row[3],
                        okved_2=row[4],
                        okved_3=row[5],
                        revenue=row[6],
                        growth_rate=row[7],
                        owner=row[8]
                    )
                return None

    async def get_all_companies(self) -> List[Company]:
        async with aiosqlite.connect(self.db_name) as db:
            async with db.execute('SELECT * FROM companies') as cursor:
                rows = await cursor.fetchall()
                return [Company(
                    inn=row[0],
                    name=row[1],
                    okved=row[2],
                    okved_1=row[3],
                    okved_2=row[4],
                    okved_3=row[5],
                    revenue=row[6],
                    growth_rate=row[7],
                    owner=row[8]
                ) for row in rows]

    async def show_results(self):
        async with aiosqlite.connect(self.db_name) as db:
            async with db.execute('SELECT * FROM companies') as cursor:
                rows = await cursor.fetchall()
                for row in rows:
                    print("=== Company {} ===".format(row[1]))
                    print(f"INN:", row[0])
                    print(f"Name:", row[1])
                    print(f"OKVED:", row[2])
                    print(f"Revenue:", row[6])
                    print(f"Growth Rate:", row[7])
                    print(f"Owner:", row[8])
                    print()

    async def get_total_companies(self) -> int:
        """Возвращает общее количество компаний в базе"""
        async with aiosqlite.connect(self.db_name) as db:
            async with db.execute('SELECT COUNT(*) FROM companies') as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0 