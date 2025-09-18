import ccxt
import pandas as pd
import asyncio
import threading
import time
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import json

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, ContextTypes

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = '1532666897:AAFtl_PbxoxI9V1v7Jjr_DYMqPUhCujDx5M'
TELEGRAM_CHAT_ID = '1305348616'  # Замените на свой ID или канал

# Параметры памп-детектора
PCT_1M_THRESHOLD = 1.5      # % рост за 1 минуту
PCT_5M_THRESHOLD = 3.0      # % рост за 5 минут
PCT_1M_DOWN_THRESHOLD = -1.5 # % падение за 1 минуту
PCT_5M_DOWN_THRESHOLD = -3.0 # % падение за 5 минут
VOLUME_MULTIPLIER = 2.5     # рост объема к среднему
MIN_24H_VOLUME_USD = 500_000

BLACKLIST = ["SCAM", "FAKE", "TEST", "DOGE", "SHIT"]
MAX_WORKERS = 8             # Уменьшено количество потоков
LOOP_SLEEP_SECONDS = 5      # Пауза между циклами
ALERT_COOLDOWN_MINUTES = 3
CACHE_REFRESH_MINUTES = 30  # Обновляем кэш каждые 30 минут
# ----------------------------------------

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)

# ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ
bot_app = None
bot_loop = None
cached_futures_symbols = []  # Кэш фьючерсных символов
last_cache_update = None
cached_ohlcv = {}            # Кэш OHLCV данных

class FuturesScreener:
    def __init__(self):
        self.exchange = ccxt.mexc({
            'options': {
                'defaultType': 'swap',
                'adjustForTimeDifference': True
            },
            'timeout': 15000,
            'enableRateLimit': True,
            'rateLimit': 500  # Быстрее запросы
        })
        self.spot_exchange = ccxt.mexc({
            'timeout': 15000,
            'enableRateLimit': True,
            'rateLimit': 500
        })
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        self.last_alerts = {}
        self.last_pullback_alerts = {}
        self.scan_counter = 0

    async def send_message_safe(self, text, reply_markup=None):
        """Безопасная отправка сообщений"""
        try:
            await bot_app.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=text,
                parse_mode="HTML",
                reply_markup=reply_markup
            )
            return True
        except Exception as e:
            logging.error(f"❌ Ошибка отправки в Telegram: {e}")
            return False

    async def send_startup_notification(self):
        """Отправка уведомления о запуске"""
        text = (
            "🚀 <b>ФЬЮЧЕРСНЫЙ СКАНЕР MEXC ЗАПУЩЕН!</b>\n"
            "📈 Сканирую фьючерсные монеты на пампы и откаты...\n"
            "⚡ Высокая волатильность = высокая прибыль!\n"
            "📲 Сигналы будут приходить сюда!"
        )
        await self.send_message_safe(text)

    async def send_scan_report(self, total_symbols, scanned_count, duration, signals_found):
        """Отправка отчета о сканировании"""
        try:
            text = (
                f"📊 <b>ОТЧЕТ СКАНИРОВАНИЯ ФЬЮЧЕРСОВ #{self.scan_counter}</b>\n\n"
                f"📈 Всего фьючерсов: <b>{total_symbols}</b>\n"
                f"🔍 Проверено: <b>{scanned_count}</b>\n"
                f"⏱️ Время: <b>{duration:.1f}</b> сек\n"
                f"🎯 Сигналов: <b>{signals_found}</b>\n"
                f"🕒 Время: <b>{datetime.now().strftime('%H:%M:%S')}</b>"
            )
            await self.send_message_safe(text)
        except Exception as e:
            logging.error(f"❌ Ошибка отправки отчета: {e}")

    async def send_signal(self, symbol, signal_data):
        """Отправка мощного торгового сигнала в Telegram"""
        try:
            signal_type = signal_data['type']
            
            leverage_info = f"🔢 Плечо: <b>x{signal_data.get('leverage', 20)}</b>\n"
            
            if signal_type == 'PUMP_LONG':
                text = (
                    f"🚀 <b>ФЬЮЧЕРСНЫЙ ПАМП ВВЕРХ</b> 🚀\n"
                    f"<code>{symbol}</code>\n\n"
                    f"CALLTYPE: <b>🔥 LONG ФЬЮЧЕРС</b>\n"
                    f"📈 1m: <b>{signal_data['pct_1m']:.2f}%</b>\n"
                    f"📈 5m: <b>{signal_data['pct_5m']:.2f}%</b>\n"
                    f"📊 Volume x{signal_data['volume_mult']:.1f}\n"
                    f"🔢 RSI: <b>{signal_data['rsi']:.2f}</b>\n"
                    f"{leverage_info}\n"
                    f"💰 Вход: <b>{signal_data['price']:.6f}</b>\n"
                    f"🛑 Стоп: <b>{signal_data['stop']:.6f}</b>\n"
                    f"🎯 TP1: <b>{signal_data['tp1']:.6f}</b>\n"
                    f"🎯 TP2: <b>{signal_data['tp2']:.6f}</b>\n"
                    f"🎯 TP3: <b>{signal_data['tp3']:.6f}</b>\n\n"
                    f"⚠️ <i>Вход только на пробой вверх!</i>"
                )
            elif signal_type == 'DUMP_SHORT':
                text = (
                    f"💣 <b>ФЬЮЧЕРСНЫЙ ДАМП ВНИЗ</b> 💣\n"
                    f"<code>{symbol}</code>\n\n"
                    f"CALLTYPE: <b>🔻 SHORT ФЬЮЧЕРС</b>\n"
                    f"📉 1m: <b>{signal_data['pct_1m']:.2f}%</b>\n"
                    f"📉 5m: <b>{signal_data['pct_5m']:.2f}%</b>\n"
                    f"📊 Volume x{signal_data['volume_mult']:.1f}\n"
                    f"🔢 RSI: <b>{signal_data['rsi']:.2f}</b>\n"
                    f"{leverage_info}\n"
                    f"💰 Вход: <b>{signal_data['price']:.6f}</b>\n"
                    f"🛑 Стоп: <b>{signal_data['stop']:.6f}</b>\n"
                    f"🎯 TP1: <b>{signal_data['tp1']:.6f}</b>\n"
                    f"🎯 TP2: <b>{signal_data['tp2']:.6f}</b>\n"
                    f"🎯 TP3: <b>{signal_data['tp3']:.6f}</b>\n\n"
                    f"⚠️ <i>Вход только на пробой вниз!</i>"
                )
            elif signal_type == 'PULLBACK_LONG':
                text = (
                    f"🎯 <b>ФЬЮЧЕРС ЛОНГ НА ОТКАТЕ</b> 📈\n"
                    f"<code>{symbol}</code>\n\n"
                    f"CALLTYPE: <b>🔁 LONG на откате</b>\n"
                    f"📊 RSI: <b>{signal_data['rsi']:.2f}</b>\n"
                    f"📊 Volume x{signal_data['volume_mult']:.1f}\n"
                    f"{leverage_info}\n"
                    f"💰 Вход: <b>{signal_data['entry']:.6f}</b>\n"
                    f"🛑 Стоп: <b>{signal_data['stop']:.6f}</b>\n"
                    f"🎯 TP1: <b>{signal_data['tp1']:.6f}</b>\n"
                    f"🎯 TP2: <b>{signal_data['tp2']:.6f}</b>\n"
                    f"🎯 TP3: <b>{signal_data['tp3']:.6f}</b>\n\n"
                    f"📉 Уровень отката: <b>{signal_data['pullback_level']:.6f}</b>\n"
                    f"📈 EMA20: <b>{signal_data['ema20']:.6f}</b>"
                )
            elif signal_type == 'PULLBACK_SHORT':
                text = (
                    f"🎯 <b>ФЬЮЧЕРС ШОРТ НА ОТКАТЕ</b> 📉\n"
                    f"<code>{symbol}</code>\n\n"
                    f"CALLTYPE: <b>🔻 SHORT на откате</b>\n"
                    f"📊 RSI: <b>{signal_data['rsi']:.2f}</b>\n"
                    f"📊 Volume x{signal_data['volume_mult']:.1f}\n"
                    f"{leverage_info}\n"
                    f"💰 Вход: <b>{signal_data['entry']:.6f}</b>\n"
                    f"🛑 Стоп: <b>{signal_data['stop']:.6f}</b>\n"
                    f"🎯 TP1: <b>{signal_data['tp1']:.6f}</b>\n"
                    f"🎯 TP2: <b>{signal_data['tp2']:.6f}</b>\n"
                    f"🎯 TP3: <b>{signal_data['tp3']:.6f}</b>\n\n"
                    f"📈 Уровень отката: <b>{signal_data['pullback_level']:.6f}</b>\n"
                    f"📉 EMA20: <b>{signal_data['ema20']:.6f}</b>"
                )

            keyboard = [
                [
                    InlineKeyboardButton("🔍 Стакан", callback_data=f"orderbook|{symbol}"),
                    InlineKeyboardButton("📊 Подробнее", callback_data=f"details|{symbol}")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await self.send_message_safe(text, reply_markup)
            logging.info(f"✅ Отправлен сигнал {signal_data['type']} для {symbol}")
            
        except Exception as e:
            logging.error(f"❌ Ошибка отправки сигнала для {symbol}: {e}")

    def send_from_thread(self, coro):
        """Отправка корутины из потока"""
        try:
            if bot_loop and bot_app:
                asyncio.run_coroutine_threadsafe(coro, bot_loop)
        except Exception as e:
            logging.error(f"❌ Ошибка отправки из потока: {e}")

    def fetch_ohlcv_with_retry(self, symbol, timeframe="1m", limit=10, retries=3):
        """Получение OHLCV с повторными попытками"""
        for attempt in range(retries):
            try:
                return self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            except ccxt.RateLimitExceeded:
                logging.warning(f"⚠️ Превышен лимит запросов для {symbol}. Попытка {attempt + 1}/{retries}")
                time.sleep(5 * (attempt + 1))  # Увеличиваем паузу после каждой попытки
        raise Exception(f"❌ Не удалось получить данные для {symbol} после {retries} попыток")

    def get_cached_ohlcv(self, symbol, timeframe="1m", limit=10):
        """Получение кэшированных OHLCV данных"""
        key = f"{symbol}_{timeframe}_{limit}"
        if key in cached_ohlcv:
            data, timestamp = cached_ohlcv[key]
            if (datetime.now() - timestamp).total_seconds() < 60:  # Кэш актуален 1 минуту
                return data
        # Если нет кэша или он устарел, загружаем новые данные
        data = self.fetch_ohlcv_with_retry(symbol, timeframe=timeframe, limit=limit)
        cached_ohlcv[key] = (data, datetime.now())
        return data

    def check_symbol(self, symbol):
        """Комплексная проверка символа"""
        try:
            logging.info(f"🔍 Проверяю фьючерс {symbol}...")
            
            # Получаем кэшированные данные OHLCV
            ohlcv_1m = self.get_cached_ohlcv(symbol, timeframe="1m", limit=10)
            ohlcv_5m = self.get_cached_ohlcv(symbol, timeframe="5m", limit=50)

            if ohlcv_1m and len(ohlcv_1m) >= 2:
                df_1m = pd.DataFrame(ohlcv_1m, columns=["ts", "o", "h", "l", "c", "v"])
                df_1m = technical_indicators(df_1m)
                
                # Проверка пампа (лонг)
                pump_signal = detect_pump_signal(df_1m)
                if pump_signal:
                    pump_signal['leverage'] = 10
                    now = datetime.now()
                    alert_key = f"{symbol}_pump"
                    if alert_key not in self.last_alerts or (now - self.last_alerts[alert_key]) > timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                        self.last_alerts[alert_key] = now
                        self.send_from_thread(self.send_signal(symbol, pump_signal))

                # Проверка дампа (шорт)
                dump_signal = detect_dump_signal(df_1m)
                if dump_signal:
                    dump_signal['leverage'] = 10
                    now = datetime.now()
                    alert_key = f"{symbol}_dump"
                    if alert_key not in self.last_alerts or (now - self.last_alerts[alert_key]) > timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                        self.last_alerts[alert_key] = now
                        self.send_from_thread(self.send_signal(symbol, dump_signal))

            if ohlcv_5m and len(ohlcv_5m) >= 50:
                df_5m = pd.DataFrame(ohlcv_5m, columns=["ts", "o", "h", "l", "c", "v"])
                
                # Лонг откат
                pullback_long_signal = detect_pullback_long_signal(df_5m, symbol)
                if pullback_long_signal:
                    pullback_long_signal['leverage'] = 20
                    now = datetime.now()
                    alert_key = f"{symbol}_pullback_long"
                    if alert_key not in self.last_pullback_alerts or (now - self.last_pullback_alerts[alert_key]) > timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                        self.last_pullback_alerts[alert_key] = now
                        self.send_from_thread(self.send_signal(symbol, pullback_long_signal))

                # Шорт откат
                pullback_short_signal = detect_pullback_short_signal(df_5m, symbol)
                if pullback_short_signal:
                    pullback_short_signal['leverage'] = 20
                    now = datetime.now()
                    alert_key = f"{symbol}_pullback_short"
                    if alert_key not in self.last_pullback_alerts or (now - self.last_pullback_alerts[alert_key]) > timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                        self.last_pullback_alerts[alert_key] = now
                        self.send_from_thread(self.send_signal(symbol, pullback_short_signal))

        except ccxt.BadSymbol:
            logging.warning(f"⚠️ Фьючерс {symbol} не доступен")
        except ccxt.RateLimitExceeded:
            logging.warning(f"⚠️ Превышен лимит запросов для {symbol}")
            time.sleep(5)
        except Exception as e:
            logging.error(f"❌ Ошибка при проверке {symbol}: {e}")

    def get_cached_futures_symbols(self):
        """Получение кэшированного списка фьючерсных символов"""
        global cached_futures_symbols, last_cache_update
        
        # Проверяем, нужно ли обновить кэш
        if (last_cache_update is None or 
            (datetime.now() - last_cache_update).total_seconds() > CACHE_REFRESH_MINUTES * 60):
            
            logging.info("🔄 Обновляю кэш фьючерсных символов...")
            cached_futures_symbols = self._load_futures_symbols()
            last_cache_update = datetime.now()
            logging.info(f"✅ Кэш обновлен: {len(cached_futures_symbols)} символов")
        
        return cached_futures_symbols

    def _load_futures_symbols(self):
        """Загрузка списка фьючерсных символов"""
        try:
            # Загружаем фьючерсные рынки
            self.exchange.load_markets()
            futures_markets = self.exchange.markets
            
            # Получаем спотовые рынки для фильтрации по объему
            spot_markets = self.spot_exchange.load_markets()
            
            futures_symbols = []
            
            # Ищем фьючерсы в формате XXX/USDT:USDT
            for symbol in futures_markets:
                if (symbol.endswith('/USDT:USDT') and 
                    futures_markets[symbol].get("active", True) and
                    not any(b.upper() in symbol.upper() for b in BLACKLIST)):
                    
                    try:
                        # Получаем базовую монету для проверки объема
                        base_coin = symbol.split('/')[0]
                        spot_symbol = f"{base_coin}/USDT"
                        
                        if spot_symbol in spot_markets:
                            ticker = self.spot_exchange.fetch_ticker(spot_symbol)
                            if ticker.get('quoteVolume', 0) > MIN_24H_VOLUME_USD:
                                futures_symbols.append(symbol)
                                
                    except Exception as e:
                        continue
                        
            logging.info(f"✅ Загружено {len(futures_symbols)} фьючерсных пар")
            return futures_symbols
            
        except Exception as e:
            logging.error(f"❌ Ошибка загрузки фьючерсов: {e}")
            return []

    def start_screener(self):
        """Основной сканер"""
        while True:
            try:
                self.scan_counter += 1
                start_time = time.time()
                
                logging.info(f"🔄 Начинаю сканирование фьючерсов #{self.scan_counter}...")
                
                # Получаем кэшированный список фьючерсных символов
                symbols = self.get_cached_futures_symbols()
                logging.info(f"📊 Найдено фьючерсов для анализа: {len(symbols)}")
                if symbols:
                    logging.info(f"📋 Первые 10: {symbols[:10]}")

                # Отправляем отчет о начале сканирования (каждый 3-й цикл)
                if self.scan_counter % 3 == 1:
                    self.send_from_thread(
                        self.send_scan_report(len(symbols), 0, 0, 0)
                    )

                # Проверяем фьючерсы
                futures = [self.executor.submit(self.check_symbol, symbol) for symbol in symbols]
                for future in futures:
                    future.result()  # Ждем завершения всех задач
                    time.sleep(0.1)  # Минимальная пауза между запросами

                end_time = time.time()
                scan_duration = end_time - start_time
                
                logging.info(f"✅ Сканирование #{self.scan_counter} завершено!")
                logging.info(f"📊 Результаты: {len(symbols)} фьючерсов, "
                           f"{scan_duration:.1f} сек")

                # Отправляем финальный отчет (каждый 5-й цикл)
                if self.scan_counter % 5 == 1:
                    self.send_from_thread(
                        self.send_scan_report(
                            len(symbols),
                            len(symbols),
                            scan_duration,
                            len(self.last_alerts) + len(self.last_pullback_alerts)
                        )
                    )

                logging.info(f"⏳ Жду {LOOP_SLEEP_SECONDS} секунд до следующего сканирования...")
                
            except Exception as e:
                logging.error(f"❌ Критическая ошибка в сканере: {e}")
                time.sleep(30)

            time.sleep(LOOP_SLEEP_SECONDS)

# ==================== ФУНКЦИИ АНАЛИЗА ====================

def technical_indicators(df):
    """Расчет технических индикаторов"""
    try:
        if len(df) < 20:
            return df
            
        df['ema12'] = df['c'].ewm(span=12).mean()
        df['ema20'] = df['c'].ewm(span=20).mean()
        df['ema26'] = df['c'].ewm(span=26).mean()
        df['ema50'] = df['c'].ewm(span=50).mean()
        df['macd'] = df['ema12'] - df['ema26']
        df['signal'] = df['macd'].ewm(span=9).mean()
        delta = df['c'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        df['avg_volume'] = df['v'].rolling(window=20).mean()
        return df
    except Exception as e:
        logging.error(f"Ошибка в technical_indicators: {e}")
        return df

def detect_pump_signal(df):
    """Детекция пампа"""
    try:
        if len(df) < 10:
            return None
        
        last = df.iloc[-1]
        prev1 = df.iloc[-2]
        prev5 = df.iloc[-6]
        
        pct_1m = (last["c"] - prev1["c"]) / prev1["c"] * 100
        pct_5m = (last["c"] - prev5["c"]) / prev5["c"] * 100
        avg_vol = df["v"].iloc[:-1].mean()
        volume_mult = last["v"] / avg_vol if avg_vol > 0 else 0
        
        if ((abs(pct_1m) > PCT_1M_THRESHOLD or abs(pct_5m) > PCT_5M_THRESHOLD) and 
            volume_mult > VOLUME_MULTIPLIER):
            
            price = last["c"]
            if pct_1m > 0 or pct_5m > 0:  # LONG
                stop = price * 0.97
                tp1 = price * 1.03
                tp2 = price * 1.06
                tp3 = price * 1.09
                signal_type = 'PUMP_LONG'
            else:  # SHORT
                stop = price * 1.03
                tp1 = price * 0.97
                tp2 = price * 0.94
                tp3 = price * 0.91
                signal_type = 'DUMP_SHORT'
            
            return {
                'type': signal_type,
                'pct_1m': pct_1m,
                'pct_5m': pct_5m,
                'volume_mult': volume_mult,
                'price': price,
                'stop': stop,
                'tp1': tp1,
                'tp2': tp2,
                'tp3': tp3,
                'rsi': last.get('rsi', 50),
                'volume': last['v']
            }
    except Exception as e:
        logging.error(f"Ошибка в detect_pump_signal: {e}")
    return None

def detect_dump_signal(df):
    """Детекция дампа"""
    try:
        if len(df) < 10:
            return None
        
        last = df.iloc[-1]
        prev1 = df.iloc[-2]
        prev5 = df.iloc[-6]
        
        pct_1m = (last["c"] - prev1["c"]) / prev1["c"] * 100
        pct_5m = (last["c"] - prev5["c"]) / prev5["c"] * 100
        avg_vol = df["v"].iloc[:-1].mean()
        volume_mult = last["v"] / avg_vol if avg_vol > 0 else 0
        
        if ((pct_1m < PCT_1M_DOWN_THRESHOLD or pct_5m < PCT_5M_DOWN_THRESHOLD) and 
            volume_mult > VOLUME_MULTIPLIER and 
            last.get('rsi', 50) > 20):
            
            price = last["c"]
            stop = price * 1.03
            tp1 = price * 0.97
            tp2 = price * 0.94
            tp3 = price * 0.91
            
            return {
                'type': 'DUMP_SHORT',
                'pct_1m': pct_1m,
                'pct_5m': pct_5m,
                'volume_mult': volume_mult,
                'price': price,
                'stop': stop,
                'tp1': tp1,
                'tp2': tp2,
                'tp3': tp3,
                'rsi': last.get('rsi', 50),
                'volume': last['v']
            }
    except Exception as e:
        logging.error(f"Ошибка в detect_dump_signal: {e}")
    return None

def detect_pullback_long_signal(df, symbol):
    """Детекция лонг отката"""
    try:
        if len(df) < 50:
            return None

        df = technical_indicators(df)
        
        last = df.iloc[-1]
        if not (last['ema12'] > last['ema26'] > last['ema50']):
            return None

        recent = df.tail(5)
        if len(recent) < 5:
            return None

        second_last = recent.iloc[-2]
        last_candle = recent.iloc[-1]
        
        if (second_last['c'] < second_last['ema20'] and 
            last_candle['c'] > last_candle['ema20'] and 
            last_candle['v'] > last_candle['avg_volume'] * 1.2):
            
            pullback_level = second_last['ema20']
            entry_price = last_candle['c']
            pullback_percent = abs(entry_price - pullback_level) / pullback_level * 100
            
            if pullback_percent <= 5.0:
                stop_loss = pullback_level * 0.98
                risk = entry_price - stop_loss
                take_profit_1 = entry_price + risk * 1
                take_profit_2 = entry_price + risk * 2
                take_profit_3 = entry_price + risk * 3

                return {
                    'type': 'PULLBACK_LONG',
                    'entry': entry_price,
                    'stop': stop_loss,
                    'tp1': take_profit_1,
                    'tp2': take_profit_2,
                    'tp3': take_profit_3,
                    'pullback_level': pullback_level,
                    'ema20': last_candle['ema20'],
                    'rsi': last_candle.get('rsi', 50),
                    'volume': last_candle['v'],
                    'volume_mult': last_candle['v'] / last_candle['avg_volume']
                }
    except Exception as e:
        logging.error(f"Ошибка в detect_pullback_long_signal: {e}")
    return None

def detect_pullback_short_signal(df, symbol):
    """Детекция шорт отката"""
    try:
        if len(df) < 50:
            return None

        df = technical_indicators(df)
        
        last = df.iloc[-1]
        if not (last['ema12'] < last['ema26'] < last['ema50']):
            return None

        recent = df.tail(5)
        if len(recent) < 5:
            return None

        second_last = recent.iloc[-2]
        last_candle = recent.iloc[-1]
        
        if (second_last['c'] > second_last['ema20'] and 
            last_candle['c'] < last_candle['ema20'] and 
            last_candle['v'] > last_candle['avg_volume'] * 1.2):
            
            pullback_level = second_last['ema20']
            entry_price = last_candle['c']
            pullback_percent = abs(entry_price - pullback_level) / pullback_level * 100
            
            if pullback_percent <= 5.0:
                stop_loss = pullback_level * 1.02
                risk = stop_loss - entry_price
                take_profit_1 = entry_price - risk * 1
                take_profit_2 = entry_price - risk * 2
                take_profit_3 = entry_price - risk * 3

                take_profit_1 = max(take_profit_1, entry_price * 0.1)
                take_profit_2 = max(take_profit_2, entry_price * 0.1)
                take_profit_3 = max(take_profit_3, entry_price * 0.1)

                return {
                    'type': 'PULLBACK_SHORT',
                    'entry': entry_price,
                    'stop': stop_loss,
                    'tp1': take_profit_1,
                    'tp2': take_profit_2,
                    'tp3': take_profit_3,
                    'pullback_level': pullback_level,
                    'ema20': last_candle['ema20'],
                    'rsi': last_candle.get('rsi', 50),
                    'volume': last_candle['v'],
                    'volume_mult': last_candle['v'] / last_candle['avg_volume']
                }
    except Exception as e:
        logging.error(f"Ошибка в detect_pullback_short_signal: {e}")
    return None

async def on_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка кнопок Telegram"""
    query = update.callback_query
    await query.answer()

    try:
        # Исправленная обработка данных - учитываем, что символ может содержать |
        query_data = query.data
        if "|" not in query_data:
            await query.message.reply_text("❌ Неверный формат данных")
            return
            
        # Разделяем только по первому | 
        parts = query_data.split("|", 1)  # Только 1 разделение
        if len(parts) != 2:
            await query.message.reply_text("❌ Неверный формат данных")
            return
            
        action = parts[0]
        symbol = parts[1]  # symbol может содержать |
        
        logging.info(f"Обрабатываю: action={action}, symbol={symbol}")
        
    except Exception as e:
        logging.error(f"❌ Ошибка парсинга данных: {e}")
        await query.message.reply_text(f"❌ Ошибка данных: {e}")
        return

    if action == "orderbook":
        try:
            exchange = ccxt.mexc({
                'options': {'defaultType': 'swap', 'adjustForTimeDifference': True},
                'timeout': 30000,
            })
            ob = exchange.fetch_order_book(symbol, limit=15)
            bids = "\n".join([f"{p:.8f} / {a:.2f}" for p, a in ob["bids"][:10]])
            asks = "\n".join([f"{p:.8f} / {a:.2f}" for p, a in ob["asks"][:10]])
            text = (
                f"🔍 <b>ФЬЮЧЕРСНЫЙ СТАКАН {symbol}</b>\n\n"
                f"<b>BIDS (покупка):</b>\n<pre>{bids}</pre>\n\n"
                f"<b>ASKS (продажа):</b>\n<pre>{asks}</pre>"
            )
            await query.message.reply_text(text, parse_mode="HTML")
        except Exception as e:
            logging.error(f"❌ Ошибка стакана для {symbol}: {e}")
            await query.message.reply_text(f"❌ Ошибка стакана: {e}")

    elif action == "details":
        try:
            exchange = ccxt.mexc({
                'options': {'defaultType': 'swap', 'adjustForTimeDifference': True},
                'timeout': 30000,
            })
            ticker = exchange.fetch_ticker(symbol)
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe="1h", limit=24)
            df = pd.DataFrame(ohlcv, columns=["ts", "o", "h", "l", "c", "v"])
            df = technical_indicators(df)
            
            last = df.iloc[-1]
            support = df['l'].tail(20).min()
            resistance = df['h'].tail(20).max()
            
            trend = "📈 ВВЕРХ" if last['ema12'] > last['ema26'] > last['ema50'] else "📉 ВНИЗ" if last['ema12'] < last['ema26'] < last['ema50'] else "횡 БОКОВИК"
            
            text = (
                f"📊 <b>ДЕТАЛИ {symbol}</b>\n\n"
                f"💰 Цена: <b>{ticker['last']:.8f}</b>\n"
                f"📈 24h High: <b>{ticker['high']:.8f}</b>\n"
                f"📉 24h Low: <b>{ticker['low']:.8f}</b>\n"
                f"📊 24h Volume: <b>${ticker['quoteVolume']:,.0f}</b>\n"
                f"🧭 Тренд: <b>{trend}</b>\n\n"
                f"🔢 RSI: <b>{last.get('rsi', 50):.2f}</b>\n"
                f"📉 MACD: <b>{last.get('macd', 0):.6f}</b>\n"
                f"📈 Signal: <b>{last.get('signal', 0):.6f}</b>\n\n"
                f"📉 Поддержка: <b>{support:.8f}</b>\n"
                f"📈 Сопротивление: <b>{resistance:.8f}</b>"
            )
            await query.message.reply_text(text, parse_mode="HTML")
        except Exception as e:
            logging.error(f"❌ Ошибка деталей для {symbol}: {e}")
            await query.message.reply_text(f"❌ Ошибка деталей: {e}")
    else:
        await query.message.reply_text("❌ Неизвестная команда")

def main():
    """Основная функция запуска"""
    global bot_app, bot_loop
    
    try:
        logging.info("🔧 Инициализирую фьючерсный сканер MEXC...")
        
        # Создаем приложение
        bot_app = Application.builder().token(TELEGRAM_TOKEN).build()
        bot_app.add_handler(CallbackQueryHandler(on_callback_query))
        
        # Получаем event loop
        try:
            bot_loop = asyncio.get_running_loop()
        except RuntimeError:
            bot_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(bot_loop)
        
        # Создаем сканер
        screener = FuturesScreener()
        
                # Запускаем сканер в отдельном потоке
        def start_screener_thread():
            time.sleep(3)  # Ждем немного для инициализации Telegram-бота
            logging.info("🚀 Запускаю фьючерсный сканер...")
            screener.start_screener()

        threading.Thread(target=start_screener_thread, daemon=True).start()

        # Отправляем уведомление о запуске
        asyncio.run_coroutine_threadsafe(
            screener.send_startup_notification(),
            bot_loop
        )

        logging.info("🚀 ФЬЮЧЕРСНЫЙ СКАНЕР MEXC ЗАПУЩЕН!")
        logging.info("📈 Сканирую фьючерсные монеты...")
        logging.info("⚡ Высокая волатильность = высокая прибыль!")
        logging.info("📲 Отправляю сигналы в Telegram...")

        # Запускаем бота
        bot_app.run_polling()

    except Exception as e:
        logging.error(f"❌ Критическая ошибка запуска: {e}")

if __name__ == "__main__":
    logging.info("🔧 Запускаю ФЬЮЧЕРСНЫЙ СКАНЕР MEXC...")
    logging.info("📊 Сканирую самые волатильные фьючерсы...")
    main()
