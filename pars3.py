import asyncio
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from database import Database, Company
import logging
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime
import time
from typing import List, Optional, Set
import aiosqlite


log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)


current_date = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
log_file = os.path.join(log_dir, f'parser_{current_date}.log')


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')


file_handler = RotatingFileHandler(
    log_file,
    maxBytes=10*1024*1024,  # 10 МБ
    backupCount=5,
    encoding='utf-8'
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)



logger.addHandler(file_handler)

def setup_driver():
    """Настройка драйвера Selenium"""
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    return webdriver.Chrome(options=options)

async def get_okved_links(driver) -> Set[str]:
    """Получение основных ссылок ОКВЭД"""
    try:
        driver.get('https://companies.rbc.ru/okved/')
        wait = WebDriverWait(driver, 10)
        okved_elements = wait.until(
            EC.presence_of_all_elements_located((By.XPATH, "//a[contains(@href, '/okved/')]"))
        )
        
        okved_links = set()
        for element in okved_elements:
            href = element.get_attribute('href')
            if href and '/okved/' in href and len(href.split('/okved/')[-1].strip('/')) == 2:
                okved_links.add(href)
        
        return okved_links
    except Exception as e:
        logger.error(f"Ошибка при получении ссылок ОКВЭД: {e}")
        return set()

async def get_sub_okved_links(driver, okved_url: str) -> Set[str]:
    """Получение под-категорий ОКВЭД"""
    try:
        driver.get(okved_url)
        wait = WebDriverWait(driver, 10)
        sub_elements = wait.until(
            EC.presence_of_all_elements_located((By.XPATH, "//a[contains(@href, '/okved/')]"))
        )
        
        sub_links = set()
        for element in sub_elements:
            href = element.get_attribute('href')
            if href and '/okved/' in href and href != okved_url and href != 'https://companies.rbc.ru/okved/':  
                    sub_links.add(href)
        return sub_links
    except Exception as e:
        logger.error(f"Ошибка при получении под-категорий ОКВЭД: {e}")
        return set()

def get_last_page(driver, url: str) -> int:
    """Определяет номер последней страницы"""
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 10)
        pagination = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".pagination__item"))
        )
        last_page = 1
        for page in pagination:
            try:
                page_num = int(page.text.strip())
                last_page = max(last_page, page_num)
            except ValueError:
                continue
        logger.info(f"Найдена последняя страница: {last_page}, будут парситься только первые {min(20,last_page)} страниц")
        return min(20,last_page)
    except Exception as e:
        logger.error(f"Error getting last page: {e}")
        return 1

def extract_company_data(company_element, cnt: int, okved: str) -> Optional[dict]:
    """Извлечение данных компании"""
    try:
        name = company_element.find_element(
            By.CSS_SELECTOR, 
            f'body > div.base-layout > main > div.base-layout__grid > div.base-layout__content > div > div:nth-child({cnt}) > p:nth-child(5)'
        ).text.strip()
        
        status = company_element.find_element(
            By.CSS_SELECTOR, 
            f'body > div.base-layout > main > div.base-layout__grid > div.base-layout__content > div > div:nth-child({cnt}) > span'
        ).text.strip()
        
        if status != 'ДЕЙСТВУЕТ':
            return None

        inn = company_element.find_element(
            By.CSS_SELECTOR, 
            f'body > div.base-layout > main > div.base-layout__grid > div.base-layout__content > div > div:nth-child({cnt}) > div:nth-child(10) > p:nth-child(1)'
        ).text.strip().split(':')[1].strip()
        
        owner = company_element.find_element(
            By.CSS_SELECTOR, 
            f'body > div.base-layout > main > div.base-layout__grid > div.base-layout__content > div > div:nth-child({cnt}) > p:nth-child(7)'
        ).text.strip().split(':')[1].strip()


        okved_parts = okved.split('.')
        okved_1 = okved_parts[0] if len(okved_parts) > 0 else ''
        okved_2 = okved_parts[1] if len(okved_parts) > 1 else ''
        okved_3 = okved_parts[2] if len(okved_parts) > 2 else ''

        company_data = {
            'name': name,
            'okved': okved,
            'okved_1': okved_1,
            'okved_2': okved_2,
            'okved_3': okved_3,
            'inn': inn,
            'owner': owner,
            'revenue': 0,
            'growth_rate': 0
        }


        info_elements = company_element.find_elements(By.CSS_SELECTOR, "div")
        for element in info_elements:
            text = element.text.strip()
            if 'Выручка:' in text:
                try:
                    rev_text = text.split(':', 1)[1].strip().split('₽')[0].replace(' ', '').replace(',', '.')
                    if rev_text !='' and 500000000<=float(rev_text)<= 15000000000:
                        company_data['revenue'] = float(rev_text)
                    elif 500000000>float(rev_text):
                        return 'small'
                    else:
                        return None
                    
                except:
                    return None
            if 'Темп прироста:' in text:
                try:
                    growth_text = text.split(':')[-1].strip()[:-1].replace(',', '.')
                    if growth_text != '-':
                        company_data['growth_rate'] = float(growth_text)
                    else:
                        company_data['growth_rate'] = '-'
                except:
                    return None

        return company_data
    except Exception as e:
        logger.error(f"Error extracting company data: {e}")
        return None

async def parse_page(driver, url: str, page: int, db: Database) -> bool:
    """
    Парсинг одной страницы с немедленным сохранением в базу.
    Возвращает False если нужно прекратить парсинг этого ОКВЭД.
    """
    try:
        if page == 1:
            full_url = url
        else:
            full_url = f"{url}{page}/"
        driver.get(full_url)
        logger.info(f"Парсинг страницы: {full_url}")
        
        wait = WebDriverWait(driver, 10)
        companies = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.company-card'))
        )
        
        okved = url.split('/okved/')[-1].strip('/')
        companies_to_save = []
        low_revenue_count = 0  # Счетчик компаний с низкой выручкой
        
        for cnt, company in enumerate(companies, 1):
            company_data = extract_company_data(company, cnt, okved)
            if company_data != 'small' and company_data != None:
                companies_to_save.append(Company(
                    name=company_data['name'],
                    okved=company_data['okved'],
                    okved_1=company_data['okved_1'],
                    okved_2=company_data['okved_2'],
                    okved_3=company_data['okved_3'],
                    inn=company_data['inn'],
                    revenue=company_data['revenue'],
                    growth_rate=company_data['growth_rate'],
                    owner=company_data['owner']
                ))
            elif company_data == 'small':
                low_revenue_count += 1
        await db.save_companies(companies_to_save)
        logger.info(f"Сохранено {len(companies_to_save)} компаний с ОКВЭД {okved}, страница {page}")

        if low_revenue_count > 60:
            logger.info(f"Прекращаем парсинг ОКВЭД {okved}: слишком много компаний с низкой выручкой")
            return False
            
        return True
            
    except Exception as e:
        logger.error(f"Error parsing page {page}: {e}")
        return True

async def process_sub_okved(driver, sub_link: str, db: Database):
    """Обработка одного ОКВЭД"""
    try:
        okved_code = sub_link.split('/okved/')[-1].strip('/')
        logger.info(f"Обработка ОКВЭД: {okved_code}")
        
        last_page = get_last_page(driver, sub_link)
        logger.info(f"Найдено {last_page} страниц для ОКВЭД {okved_code}")
        
        for page in range(1, last_page + 1):
            logger.info(f"Парсинг страницы {page}/{last_page} для ОКВЭД {okved_code}")
            continue_parsing = await parse_page(driver, sub_link, page, db)
            
            if not continue_parsing:
                logger.info(f"Досрочное завершение парсинга ОКВЭД {okved_code}")
                break
                
            await asyncio.sleep(2)
            
    except Exception as e:
        logger.error(f"Ошибка при обработке ОКВЭД {sub_link}: {e}")

async def process_driver_links(driver, links: List[str], db: Database):
    """Обработка списка ссылок одним драйвером"""
    for link in links:
        await process_sub_okved(driver, link, db)
        await asyncio.sleep(5)

async def get_processed_okveds(db: Database) -> Set[str]:
    """Получает список уже обработанных ОКВЭД из базы данных"""
    async with aiosqlite.connect(db.db_name) as db_conn:
        async with db_conn.execute('''
            SELECT DISTINCT okved_1, okved_2 
            FROM companies 
            WHERE okved_1 IS NOT NULL 
            AND okved_2 IS NOT NULL
        ''') as cursor:
            rows = await cursor.fetchall()
            return {f"{row[0]}|{row[1]}" for row in rows}


async def filter_sub_okved_links(sub_links: Set[str], db: Database) -> Set[str]:
    """Фильтрует ссылки ОКВЭД, оставляя только те, которые нужно парсить"""
    filtered_links = set()
    processed_okveds = await get_processed_okveds(db)
    
    for link in sub_links:
        okved = link.split('/okved/')[-1].strip('/')
        parts = okved.split('.')
        
        if len(parts) < 2:
            continue
            
        okved_key = f"{parts[0]}|{parts[1]}"

        if okved_key in processed_okveds:
            logger.info(f"Пропускаем ОКВЭД {okved} (уже в базе)")
            continue
        
        filtered_links.add(link)
        
    return filtered_links

async def main():
    db = Database()
    await db.create_table()
    initial_count = await db.get_total_companies()
    logger.info(f"Начальное количество компаний в БД: {initial_count}")

    num_drivers = 3
    drivers = [setup_driver() for _ in range(num_drivers)]
    
    try:
        main_okved = ['07']
        main_okved_links = [f"https://companies.rbc.ru/okved/{okved}/" for okved in main_okved]
        logger.info(f"Используем {len(main_okved)} основных ОКВЭД категорий")
        
        # Собираем все под-категории
        all_sub_links = set()
        for main_link in main_okved_links:
            sub_links = await get_sub_okved_links(drivers[0], main_link)
            all_sub_links.update(sub_links)
        
        # Фильтруем ссылки(если надо, тут я не фильитрую), оставляя только те, которые нужно парсить
        filtered_sub_links = all_sub_links
        
        logger.info(f"Всего найдено {len(all_sub_links)} под-категорий ОКВЭД")
        logger.info(f"После фильтрации осталось {len(filtered_sub_links)} под-категорий для парсинга")
        
        if not filtered_sub_links:
            logger.info("Нет новых ОКВЭД для парсинга")
            return
        
        # Разделяем отфильтрованные ссылки между драйверами
        sub_links_list = sorted(list(filtered_sub_links))
        links_per_driver = max(1, len(sub_links_list) // num_drivers)
        driver_tasks = []
        
        for i in range(num_drivers):
            start_idx = i * links_per_driver
            end_idx = start_idx + links_per_driver if i < num_drivers - 1 else len(sub_links_list)
            driver_links = sub_links_list[start_idx:end_idx]
            if driver_links:
                driver_tasks.append(process_driver_links(drivers[i], driver_links, db))
        
        # Запускаем все задачи параллельно
        if driver_tasks:
            await asyncio.gather(*driver_tasks)
        
        final_count = await db.get_total_companies()
        logger.info(f"Парсинг завершен")
        logger.info(f"Было в базе: {initial_count} компаний")
        logger.info(f"Добавлено новых: {final_count - initial_count} компаний")
        logger.info(f"Всего в базе: {final_count} компаний")
        
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")
    finally:
        for driver in drivers:
            driver.quit()

if __name__ == "__main__":
    asyncio.run(main())
