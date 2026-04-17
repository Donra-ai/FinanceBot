import os
import re
import time
import json
import base64
import logging
import anthropic
import gspread
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, MessageHandler, CommandHandler,
    CallbackQueryHandler, filters, ContextTypes
)

load_dotenv()

# ── Variables de entorno ──────────────────────────────────────────────────────
TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
GOOGLE_SHEETS_ID  = os.getenv("GOOGLE_SHEETS_ID")
GOOGLE_SHEETS_CREDENTIALS_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS")

SHEET_NAME        = "Día a Día"
HEADER_ROW        = 2  # Los encabezados están en fila 2
DEDUP_WINDOW      = 20

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY, timeout=30.0, max_retries=2)

CATEGORIAS   = ["Comida", "Transporte", "Entretenimiento", "Salud", "Deuda", "Servicios", "Compras", "Otro"]
METODOS_PAGO = ["Nequi", "Daviplata", "Bancolombia", "Tarjeta Visa", "Tarjeta Mastercard", "Efectivo", "Otro"]

# ── Autenticación con Google Sheets ───────────────────────────────────────────

def get_gs_client():
    """
    Autentica con Google Sheets usando las credenciales de Service Account.
    Retorna el cliente autenticado de gspread.
    """
    try:
        # Parse las credenciales JSON desde la variable de entorno
        credentials_dict = json.loads(GOOGLE_SHEETS_CREDENTIALS_JSON)
        credentials = Credentials.from_service_account_info(
            credentials_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        )
        return gspread.authorize(credentials)
    except Exception as e:
        logger.error(f"Error autenticando con Google Sheets: {e}")
        raise

def get_worksheet():
    """
    Abre la Google Sheet y retorna el worksheet específico.
    """
    try:
        gs = get_gs_client()
        sheet = gs.open_by_key(GOOGLE_SHEETS_ID)
        ws = sheet.worksheet(SHEET_NAME)
        return ws
    except Exception as e:
        logger.error(f"Error abriendo worksheet: {e}")
        raise

# ── Prompt ────────────────────────────────────────────────────────────────────

def get_system_prompt() -> str:
    año_actual = datetime.today().year
    return f"""Eres un asistente especializado en extraer información financiera de comprobantes de pago colombianos.
A partir de la imagen recibida, extrae SIEMPRE estos campos en formato JSON exacto:

{{"fecha":"DD/MM/YYYY","valor":12345.00,"categoria":"Comida","metodo_pago":"Nequi","nota":"Comercio - descripción breve"}}

Reglas estrictas:
- fecha: usa la fecha del comprobante, NO la de hoy. Formato DD/MM/YYYY.
  IMPORTANTE: El año actual es {año_actual}. Si el comprobante no muestra el año explícitamente, usa {año_actual}.
  Ejemplo: si dice "18 de mar" → fecha: "18/03/{año_actual}"
- valor: número puro sin símbolos, sin puntos de miles, sin comas. Ejemplo: 23000 (no $23.000).
- categoria: SOLO una de estas: Comida, Transporte, Entretenimiento, Salud, Deuda, Servicios, Compras, Otro
- metodo_pago: SOLO una de estas: Nequi, Daviplata, Bancolombia, Tarjeta Visa, Tarjeta Mastercard, Efectivo, Otro
- nota: nombre del comercio o destinatario y descripción breve.

Para comprobantes Nequi → metodo_pago: "Nequi"
Para transferencias Bancolombia → metodo_pago: "Bancolombia"
Para compras Amazon → categoria: "Compras", metodo_pago según tarjeta usada o "Otro"
Para transferencias a personas → categoria: "Otro" o la que corresponda según contexto

IMPORTANTE: Responde ÚNICAMENTE con el objeto JSON en una sola línea. Sin markdown, sin ```json, sin explicaciones."""

# ── Helpers: formato ─────────────────────────────────────────────────────────

def escape_md(text: str) -> str:
    special = r"\_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(special)}])", r"\\\1", str(text))

def fmt_valor(v: float) -> str:
    return f"${v:,.0f}"

def build_resumen(data: dict) -> str:
    return (
        f"✅ *Gasto registrado correctamente*\n\n"
        f"📅 *Fecha:* {escape_md(data['fecha'])}\n"
        f"💰 *Valor:* {escape_md(fmt_valor(data['valor']))}\n"
        f"🏷️ *Categoría:* {escape_md(data['categoria'])}\n"
        f"💳 *Método de pago:* {escape_md(data['metodo_pago'])}\n"
        f"📝 *Nota:* {escape_md(data['nota'])}"
    )

# ── Helpers: Claude ───────────────────────────────────────────────────────────

def parse_claude_response(raw: str) -> dict:
    raw = raw.strip()
    logger.info(f"Claude raw response: {raw}")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    fenced = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
    fenced = re.sub(r"\s*```$", "", fenced).strip()
    try:
        return json.loads(fenced)
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{[^{}]+\}", raw, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    raise ValueError(f"No se pudo parsear JSON: {raw[:200]}")

def normalize_data(data: dict) -> dict:
    valor_raw = str(data.get("valor", "0"))
    valor_clean = re.sub(r"[^\d,.]", "", valor_raw)
    if "," in valor_clean and "." in valor_clean:
        valor_clean = valor_clean.replace(".", "").replace(",", ".")
    elif "," in valor_clean:
        parts = valor_clean.split(",")
        valor_clean = valor_clean.replace(",", ".") if len(parts[-1]) <= 2 else valor_clean.replace(",", "")
    try:
        data["valor"] = float(valor_clean)
    except ValueError:
        data["valor"] = 0.0
    if data.get("categoria") not in set(CATEGORIAS):
        data["categoria"] = "Otro"
    if data.get("metodo_pago") not in set(METODOS_PAGO):
        data["metodo_pago"] = "Otro"
    if not data.get("fecha"):
        data["fecha"] = datetime.today().strftime("%d/%m/%Y")
    if not data.get("nota"):
        data["nota"] = "Sin descripción"
    return data

def extract_data_from_image(image_bytes: bytes, mime_type: str) -> dict:
    image_b64 = base64.standard_b64encode(image_bytes).decode("utf-8")
    msg = client.messages.create(
        model="claude-opus-4-5", max_tokens=500, system=get_system_prompt(),
        messages=[{"role": "user", "content": [
            {"type": "image", "source": {"type": "base64", "media_type": mime_type, "data": image_b64}},
            {"type": "text", "text": "Extrae la información financiera de este comprobante y responde SOLO con el JSON."}
        ]}],
    )
    return normalize_data(parse_claude_response(msg.content[0].text.strip()))

# ── Helpers: Google Sheets ────────────────────────────────────────────────────

def get_first_empty_row(ws) -> int:
    """
    Encuentra la primera fila vacía en la Google Sheet.
    """
    all_values = ws.get_all_values()
    for idx, row in enumerate(all_values[HEADER_ROW:], start=HEADER_ROW + 1):
        if all(cell == "" for cell in row[:5]):  # Primeras 5 columnas
            return idx + 1  # gspread usa 1-indexed
    return len(all_values) + 1

def row_is_duplicate(ws, data: dict):
    """
    Verifica si el gasto ya está registrado (últimas 20 filas).
    """
    all_values = ws.get_all_values()
    max_r = len(all_values)
    start = max(HEADER_ROW, max_r - DEDUP_WINDOW)

    for idx in range(start, max_r):
        row = all_values[idx]
        if len(row) >= 4:
            fc = row[0]  # Fecha
            vc = row[1]  # Valor
            mc = row[3]  # Método de pago

            try:
                if (str(fc) == str(data["fecha"])
                        and vc != ""
                        and abs(float(vc) - data["valor"]) < 0.01
                        and str(mc) == str(data["metodo_pago"])):
                    return True, idx + 1
            except (ValueError, IndexError):
                continue

    return False, -1

def write_to_google_sheet(data: dict, retries=5):
    """
    Escribe los datos del gasto en la Google Sheet.
    Retorna (número_de_fila, es_duplicado).
    """
    for attempt in range(retries):
        try:
            ws = get_worksheet()
            is_dup, dup_row = row_is_duplicate(ws, data)
            if is_dup:
                return dup_row, True

            nr = get_first_empty_row(ws)

            # Escribir en Google Sheets (1-indexed)
            ws.update(f'A{nr}', data['fecha'])
            ws.update(f'B{nr}', data['valor'])
            ws.update(f'C{nr}', data['categoria'])
            ws.update(f'D{nr}', data['metodo_pago'])
            ws.update(f'E{nr}', data['nota'])

            logger.info(f"Guardado fila {nr}: {data}")
            return nr, False

        except Exception as e:
            if attempt == retries - 1:
                logger.error(f"Error escribiendo en Google Sheet: {e}")
                raise
            logger.warning(f"Error al escribir, reintento {attempt+1}/{retries}...")
            time.sleep(3)

def read_row(ws, row: int) -> dict:
    """
    Lee una fila de la Google Sheet.
    """
    all_values = ws.get_all_values()
    if row - 1 < len(all_values):
        row_data = all_values[row - 1]
        return {
            "row": row,
            "fecha":      str(row_data[0] if len(row_data) > 0 else ""),
            "valor":      float(row_data[1] if len(row_data) > 1 and row_data[1] else 0),
            "categoria":  str(row_data[2] if len(row_data) > 2 else ""),
            "metodo_pago":str(row_data[3] if len(row_data) > 3 else ""),
            "nota":       str(row_data[4] if len(row_data) > 4 else ""),
        }
    return None

def find_last_row(ws) -> int | None:
    """
    Encuentra la última fila con datos.
    """
    all_values = ws.get_all_values()
    for idx in range(len(all_values) - 1, HEADER_ROW - 1, -1):
        if idx < len(all_values) and len(all_values[idx]) > 0 and all_values[idx][0]:
            return idx + 1
    return None

def update_cell(row: int, col: int, value, retries=5):
    """
    Actualiza una celda en la Google Sheet.
    col: 1=Fecha, 2=Valor, 3=Categoría, 4=Método, 5=Nota
    """
    cols = ['A', 'B', 'C', 'D', 'E']
    col_letter = cols[col - 1]

    for attempt in range(retries):
        try:
            ws = get_worksheet()
            ws.update(f'{col_letter}{row}', value)
            return
        except Exception as e:
            if attempt == retries - 1:
                logger.error(f"Error actualizando celda: {e}")
                raise
            time.sleep(3)

def delete_row_data(row: int, retries=5):
    """
    Elimina los datos de una fila (deja vacía).
    """
    for attempt in range(retries):
        try:
            ws = get_worksheet()
            data = read_row(ws, row)
            for col in range(1, 6):
                update_cell(row, col, "")
            return data
        except Exception as e:
            if attempt == retries - 1:
                logger.error(f"Error eliminando fila: {e}")
                raise
            time.sleep(3)

# ── Helpers: consultas Google Sheets ──────────────────────────────────────────

def rows_in_range(ws, desde: str, hasta: str):
    """
    Retorna filas dentro de un rango de fechas.
    """
    def parse_date(s):
        try:
            return datetime.strptime(str(s), "%d/%m/%Y")
        except Exception:
            return None

    d0 = datetime.strptime(desde, "%d/%m/%Y")
    d1 = datetime.strptime(hasta, "%d/%m/%Y")
    all_values = ws.get_all_values()

    for idx in range(HEADER_ROW, len(all_values)):
        row = all_values[idx]
        if len(row) >= 2:
            fecha = row[0]
            valor = row[1]
            if fecha and valor:
                fd = parse_date(fecha)
                if fd and d0 <= fd <= d1:
                    yield {
                        "fecha":      str(fecha),
                        "valor":      float(valor) if valor else 0,
                        "categoria":  str(row[2] if len(row) > 2 else ""),
                        "metodo_pago":str(row[3] if len(row) > 3 else ""),
                        "nota":       str(row[4] if len(row) > 4 else ""),
                    }

def sum_range(ws, desde: str, hasta: str):
    total, count = 0.0, 0
    for r in rows_in_range(ws, desde, hasta):
        total += r["valor"]
        count += 1
    return total, count

def sum_by_cat(ws, desde: str, hasta: str):
    totals = {}
    for r in rows_in_range(ws, desde, hasta):
        cat = r["categoria"] or "Otro"
        totals[cat] = totals.get(cat, 0.0) + r["valor"]
    return dict(sorted(totals.items(), key=lambda x: -x[1]))

def sum_by_metodo(ws, desde: str, hasta: str):
    totals = {}
    for r in rows_in_range(ws, desde, hasta):
        m = r["metodo_pago"] or "Otro"
        totals[m] = totals.get(m, 0.0) + r["valor"]
    return dict(sorted(totals.items(), key=lambda x: -x[1]))

def top_n(ws, desde: str, hasta: str, n=5):
    rows = list(rows_in_range(ws, desde, hasta))
    return sorted(rows, key=lambda x: -x["valor"])[:n]

def search_rows(ws, query: str, limit=10):
    q = query.lower()
    results = []
    all_values = ws.get_all_values()

    for idx in range(HEADER_ROW, len(all_values)):
        row = all_values[idx]
        if len(row) > 4:
            nota = str(row[4]).lower()
            if q in nota:
                results.append(read_row(ws, idx + 1))
                if len(results) >= limit:
                    break
    return results

# ── Keyboard builders ─────────────────────────────────────────────────────────

def kb_post_registro(excel_row: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✏️ Categoría", callback_data=f"edit_cat|{excel_row}"),
            InlineKeyboardButton("📝 Nota",       callback_data=f"edit_nota|{excel_row}"),
        ],
        [
            InlineKeyboardButton("💰 Valor",  callback_data=f"edit_valor|{excel_row}"),
            InlineKeyboardButton("📅 Fecha",  callback_data=f"edit_fecha|{excel_row}"),
        ],
        [InlineKeyboardButton("🗑️ Borrar registro", callback_data=f"delete|{excel_row}")],
    ])

def kb_categorias(excel_row: int) -> InlineKeyboardMarkup:
    buttons = []
    for i in range(0, len(CATEGORIAS), 2):
        row_btns = [InlineKeyboardButton(CATEGORIAS[i], callback_data=f"setcat|{excel_row}|{CATEGORIAS[i]}")]
        if i + 1 < len(CATEGORIAS):
            row_btns.append(InlineKeyboardButton(CATEGORIAS[i+1], callback_data=f"setcat|{excel_row}|{CATEGORIAS[i+1]}"))
        buttons.append(row_btns)
    buttons.append([InlineKeyboardButton("← Volver", callback_data=f"back|{excel_row}")])
    return InlineKeyboardMarkup(buttons)

def kb_metodos(excel_row: int) -> InlineKeyboardMarkup:
    buttons = []
    for i in range(0, len(METODOS_PAGO), 2):
        row_btns = [InlineKeyboardButton(METODOS_PAGO[i], callback_data=f"setmet|{excel_row}|{METODOS_PAGO[i]}")]
        if i + 1 < len(METODOS_PAGO):
            row_btns.append(InlineKeyboardButton(METODOS_PAGO[i+1], callback_data=f"setmet|{excel_row}|{METODOS_PAGO[i+1]}"))
        buttons.append(row_btns)
    buttons.append([InlineKeyboardButton("← Volver", callback_data=f"back|{excel_row}")])
    return InlineKeyboardMarkup(buttons)

# ── Handler: fotos ────────────────────────────────────────────────────────────

async def process_image(update: Update, image_bytes: bytes, mime_type: str) -> None:
    try:
        data = extract_data_from_image(image_bytes, mime_type)
    except ValueError as e:
        logger.error(f"Parse error: {e}")
        await update.message.reply_text("⚠️ Respuesta inesperada de Claude. Reenvía la foto.")
        return
    except anthropic.APITimeoutError:
        await update.message.reply_text("⏱️ Claude tardó demasiado. Reenvía el comprobante en unos segundos.")
        return
    except anthropic.APIConnectionError as e:
        logger.error(f"Conexión: {e}")
        await update.message.reply_text("🌐 Sin conexión con Claude. Verifica tu internet.")
        return
    except anthropic.BadRequestError as e:
        logger.error(f"BadRequest: {e}")
        await update.message.reply_text("❌ Imagen no procesable. Asegúrate de que el comprobante sea legible.")
        return
    except anthropic.AuthenticationError:
        await update.message.reply_text("❌ API Key inválida. Revisa el .env.")
        return
    except Exception as e:
        logger.error(f"Error Claude: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Error inesperado: {type(e).__name__}: {e}")
        return

    try:
        excel_row, is_dup = write_to_google_sheet(data)
    except Exception as e:
        logger.error(f"Error Google Sheet: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Error al guardar: {type(e).__name__}. Intenta de nuevo.")
        return

    if is_dup:
        await update.message.reply_text(
            f"⚠️ *Comprobante ya registrado* \\(fila {excel_row}\\)\n\n"
            f"📅 {escape_md(data['fecha'])}  💰 {escape_md(fmt_valor(data['valor']))}  💳 {escape_md(data['metodo_pago'])}\n\n"
            f"_No se guardó de nuevo\\._",
            parse_mode="MarkdownV2"
        )
        return

    await update.message.reply_text(
        build_resumen(data) + f"\n\n_¿Algo está mal\\? Edítalo aquí:_",
        parse_mode="MarkdownV2",
        reply_markup=kb_post_registro(excel_row)
    )

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("📸 Comprobante recibido. Procesando con Claude...")
    try:
        photo = update.message.photo[-1]
        file = await context.bot.get_file(photo.file_id)
        image_bytes = bytes(await file.download_as_bytearray())
    except Exception as e:
        logger.error(f"Error foto: {e}")
        await update.message.reply_text("❌ No pude descargar la imagen.")
        return
    await process_image(update, image_bytes, "image/jpeg")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    doc = update.message.document
    mime_type = doc.mime_type or ""
    if not mime_type.startswith("image/"):
        await update.message.reply_text("⚠️ Solo proceso imágenes.")
        return
    await update.message.reply_text("📸 Imagen recibida. Procesando con Claude...")
    try:
        file = await context.bot.get_file(doc.file_id)
        image_bytes = bytes(await file.download_as_bytearray())
    except Exception as e:
        logger.error(f"Error doc: {e}")
        await update.message.reply_text("❌ No pude descargar la imagen.")
        return
    await process_image(update, image_bytes, mime_type)

# ── Handler: botones inline ───────────────────────────────────────────────────

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    parts = query.data.split("|")
    action = parts[0]
    excel_row = int(parts[1]) if len(parts) > 1 else None

    if action == "edit_cat":
        await query.edit_message_reply_markup(reply_markup=kb_categorias(excel_row))

    elif action == "setcat":
        nueva_cat = parts[2]
        try:
            update_cell(excel_row, 3, nueva_cat)
        except Exception as e:
            await query.edit_message_text(f"⚠️ Error: {e}")
            return
        ws = get_worksheet()
        data = read_row(ws, excel_row)
        await query.edit_message_text(
            build_resumen(data) + f"\n\n_¿Algo está mal\\? Edítalo aquí:_",
            parse_mode="MarkdownV2",
            reply_markup=kb_post_registro(excel_row)
        )

    elif action == "edit_nota":
        context.user_data["esperando"] = ("nota", excel_row)
        await query.edit_message_text(
            "📝 Escribe la nueva nota para este registro:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖ Cancelar", callback_data=f"cancelar|{excel_row}|nota")]])
        )

    elif action == "edit_valor":
        context.user_data["esperando"] = ("valor", excel_row)
        await query.edit_message_text(
            "💰 Escribe el nuevo valor \\(solo números, sin símbolos\\):",
            parse_mode="MarkdownV2",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖ Cancelar", callback_data=f"cancelar|{excel_row}|valor")]])
        )

    elif action == "edit_fecha":
        context.user_data["esperando"] = ("fecha", excel_row)
        await query.edit_message_text(
            "📅 Escribe la nueva fecha en formato DD/MM/YYYY:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖ Cancelar", callback_data=f"cancelar|{excel_row}|fecha")]])
        )

    elif action == "cancelar":
        context.user_data.pop("esperando", None)
        try:
            ws = get_worksheet()
            data = read_row(ws, excel_row)
            await query.edit_message_text(
                build_resumen(data) + f"\n\n_¿Algo está mal\\? Edítalo aquí:_",
                parse_mode="MarkdownV2",
                reply_markup=kb_post_registro(excel_row)
            )
        except Exception:
            await query.edit_message_text("↩️ Cancelado.")

    elif action == "back":
        try:
            ws = get_worksheet()
            data = read_row(ws, excel_row)
            await query.edit_message_text(
                build_resumen(data) + f"\n\n_¿Algo está mal\\? Edítalo aquí:_",
                parse_mode="MarkdownV2",
                reply_markup=kb_post_registro(excel_row)
            )
        except Exception as e:
            await query.edit_message_text(f"❌ Error: {e}")

    elif action == "delete":
        try:
            deleted = delete_row_data(excel_row)
        except Exception as e:
            await query.edit_message_text(f"⚠️ Error: {e}")
            return
        await query.edit_message_text(
            f"🗑️ *Registro eliminado*\n\n"
            f"📅 {escape_md(deleted['fecha'])}  💰 {escape_md(fmt_valor(deleted['valor']))}  💳 {escape_md(deleted['metodo_pago'])}\n"
            f"📝 {escape_md(deleted['nota'])}",
            parse_mode="MarkdownV2"
        )
        logger.info(f"Eliminado fila {excel_row}: {deleted}")

# ── Handler: texto libre (edición pendiente o fallback) ───────────────────────

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    esperando = context.user_data.get("esperando")

    if esperando:
        campo, excel_row = esperando
        texto = update.message.text.strip()

        if campo == "nota":
            try:
                update_cell(excel_row, 5, texto)
            except Exception as e:
                await update.message.reply_text(f"⚠️ Error: {e}")
                return

        elif campo == "valor":
            valor_clean = re.sub(r"[^\d,.]", "", texto)
            if "," in valor_clean and "." in valor_clean:
                valor_clean = valor_clean.replace(".", "").replace(",", ".")
            elif "," in valor_clean:
                parts = valor_clean.split(",")
                valor_clean = valor_clean.replace(",", ".") if len(parts[-1]) <= 2 else valor_clean.replace(",", "")
            try:
                valor_float = float(valor_clean)
                update_cell(excel_row, 2, valor_float)
            except (ValueError, Exception) as e:
                await update.message.reply_text(f"❌ Valor inválido: {e}")
                return

        elif campo == "fecha":
            try:
                datetime.strptime(texto, "%d/%m/%Y")
                update_cell(excel_row, 1, texto)
            except ValueError:
                await update.message.reply_text("❌ Formato incorrecto. Usa DD/MM/YYYY — ej: 28/03/2026")
                return
            except Exception as e:
                await update.message.reply_text(f"⚠️ Error: {e}")
                return

        context.user_data.pop("esperando", None)
        ws = get_worksheet()
        data = read_row(ws, excel_row)
        await update.message.reply_text(
            f"✅ *{campo.capitalize()} actualizado*\n\n" + build_resumen(data),
            parse_mode="MarkdownV2",
            reply_markup=kb_post_registro(excel_row)
        )
        return

    await update.message.reply_text(
        "📷 Envíame una foto de tu comprobante para registrar el gasto\\.\n\n"
        "Usa /ayuda para ver todos los comandos disponibles\\.",
        parse_mode="MarkdownV2"
    )

# ── Comandos ──────────────────────────────────────────────────────────────────

AYUDA_MSG = (
    "🤖 *FinanceBot Donra — Comandos*\n\n"
    "📷 *Foto* → registra el gasto automáticamente\n\n"
    "📊 *Resúmenes:*\n"
    "/hoy — gastos de hoy\n"
    "/ayer — gastos de ayer\n"
    "/semana — últimos 7 días\n"
    "/mes — mes en curso\n"
    "/quincena — quincena actual\n"
    "/año — año completo\n\n"
    "🏷️ *Por categoría o método:*\n"
    "/cat \\[nombre\\] — ej: /cat comida\n"
    "/metodo \\[nombre\\] — ej: /metodo nequi\n"
    "/top5 — 5 gastos más altos del mes\n"
    "/promedio — gasto diario promedio del mes\n\n"
    "🔍 *Búsqueda:*\n"
    "/buscar \\[texto\\] — ej: /buscar rappi\n"
    "/ultimo — último registro\n\n"
    "✏️ *Edición:*\n"
    "/editar — editar último registro\n"
    "/borrar — eliminar último registro"
)

async def cmd_ayuda(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(AYUDA_MSG, parse_mode="MarkdownV2")

async def cmd_ultimo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        ws = get_worksheet()
        last_row = find_last_row(ws)
        data = read_row(ws, last_row) if last_row else None
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        return
    if not data:
        await update.message.reply_text("📭 No hay registros todavía.")
        return
    await update.message.reply_text(
        f"📋 *Último registro \\(fila {data['row']}\\)*\n\n" + build_resumen(data)[build_resumen(data).index("\n\n")+2:],
        parse_mode="MarkdownV2",
        reply_markup=kb_post_registro(data["row"])
    )

async def cmd_hoy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    hoy = datetime.today().strftime("%d/%m/%Y")
    try:
        ws = get_worksheet()
        total, count = sum_range(ws, hoy, hoy)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        return
    if count == 0:
        await update.message.reply_text(f"📭 Sin gastos hoy \\({escape_md(hoy)}\\)\\.", parse_mode="MarkdownV2")
        return
    await update.message.reply_text(
        f"📊 *Hoy {escape_md(hoy)}*\n\n💰 *Total:* {escape_md(fmt_valor(total))}\n🔢 *Transacciones:* {count}",
        parse_mode="MarkdownV2"
    )

async def cmd_ayer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ayer = (datetime.today() - timedelta(days=1)).strftime("%d/%m/%Y")
    try:
        ws = get_worksheet()
        total, count = sum_range(ws, ayer, ayer)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        return
    if count == 0:
        await update.message.reply_text(f"📭 Sin gastos ayer \\({escape_md(ayer)}\\)\\.", parse_mode="MarkdownV2")
        return
    await update.message.reply_text(
        f"📊 *Ayer {escape_md(ayer)}*\n\n💰 *Total:* {escape_md(fmt_valor(total))}\n🔢 *Transacciones:* {count}",
        parse_mode="MarkdownV2"
    )

async def cmd_semana(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    hasta = datetime.today()
    desde = hasta - timedelta(days=6)
    d0, d1 = desde.strftime("%d/%m/%Y"), hasta.strftime("%d/%m/%Y")
    try:
        ws = get_worksheet()
        total, count = sum_range(ws, d0, d1)
        por_cat = sum_by_cat(ws, d0, d1)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        return
    if count == 0:
        await update.message.reply_text("📭 Sin gastos en los últimos 7 días.")
        return
    detalle = "\n".join(f"  {escape_md(k)}: {escape_md(fmt_valor(v))}" for k, v in list(por_cat.items())[:5])
    await update.message.reply_text(
        f"📊 *Últimos 7 días*\n_{escape_md(d0)} → {escape_md(d1)}_\n\n"
        f"💰 *Total:* {escape_md(fmt_valor(total))}\n🔢 *Transacciones:* {count}\n\n"
        f"*Por categoría:*\n{detalle}",
        parse_mode="MarkdownV2"
    )

async def cmd_mes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    hoy = datetime.today()
    d0 = hoy.replace(day=1).strftime("%d/%m/%Y")
    d1 = hoy.strftime("%d/%m/%Y")
    mes_label = hoy.strftime("%B %Y")
    try:
        ws = get_worksheet()
        total, count = sum_range(ws, d0, d1)
        por_cat = sum_by_cat(ws, d0, d1)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        return
    if count == 0:
        await update.message.reply_text(f"📭 Sin gastos en {escape_md(mes_label)}\\.", parse_mode="MarkdownV2")
        return
    detalle = "\n".join(f"  {escape_md(k)}: {escape_md(fmt_valor(v))}" for k, v in list(por_cat.items())[:5])
    await update.message.reply_text(
        f"📊 *{escape_md(mes_label)}*\n\n"
        f"💰 *Total:* {escape_md(fmt_valor(total))}\n🔢 *Transacciones:* {count}\n\n"
        f"*Por categoría:*\n{detalle}",
        parse_mode="MarkdownV2"
    )

async def cmd_quincena(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    hoy = datetime.today()
    if hoy.day <= 15:
        d0 = hoy.replace(day=1).strftime("%d/%m/%Y")
        d1 = hoy.replace(day=15).strftime("%d/%m/%Y")
        label = f"1ra quincena {hoy.strftime('%B %Y')}"
    else:
        import calendar
        ultimo = calendar.monthrange(hoy.year, hoy.month)[1]
        d0 = hoy.replace(day=16).strftime("%d/%m/%Y")
        d1 = hoy.replace(day=ultimo).strftime("%d/%m/%Y")
        label = f"2da quincena {hoy.strftime('%B %Y')}"
    try:
        ws = get_worksheet()
        total, count = sum_range(ws, d0, d1)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        return
    if count == 0:
        await update.message.reply_text(f"📭 Sin gastos en {escape_md(label)}\\.", parse_mode="MarkdownV2")
        return
    await update.message.reply_text(
        f"📊 *{escape_md(label)}*\n\n💰 *Total:* {escape_md(fmt_valor(total))}\n🔢 *Transacciones:* {count}",
        parse_mode="MarkdownV2"
    )

async def cmd_año(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    hoy = datetime.today()
    d0 = hoy.replace(month=1, day=1).strftime("%d/%m/%Y")
    d1 = hoy.strftime("%d/%m/%Y")
    try:
        ws = get_worksheet()
        total, count = sum_range(ws, d0, d1)
        por_cat = sum_by_cat(ws, d0, d1)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        return
    if count == 0:
        await update.message.reply_text(f"📭 Sin gastos en {hoy.year}\\.")
        return
    detalle = "\n".join(f"  {escape_md(k)}: {escape_md(fmt_valor(v))}" for k, v in list(por_cat.items())[:6])
    await update.message.reply_text(
        f"📊 *Año {hoy.year}*\n\n💰 *Total:* {escape_md(fmt_valor(total))}\n🔢 *Transacciones:* {count}\n\n"
        f"*Por categoría:*\n{detalle}",
        parse_mode="MarkdownV2"
    )

async def cmd_cat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args
    if not args:
        cats_list = escape_md(", ".join(CATEGORIAS))
        await update.message.reply_text(
            f"Uso: /cat \\[categoría\\]\nEjemplo: /cat comida\n\nCategorías: {cats_list}",
            parse_mode="MarkdownV2"
        )
        return
    query_cat = " ".join(args).strip().title()
    match = next((c for c in CATEGORIAS if c.lower() == query_cat.lower()), None)
    if not match:
        await update.message.reply_text(f"❌ Categoría no reconocida: {query_cat}")
        return
    hoy = datetime.today()
    d0 = hoy.replace(day=1).strftime("%d/%m/%Y")
    d1 = hoy.strftime("%d/%m/%Y")
    try:
        ws = get_worksheet()
        rows = [r for r in rows_in_range(ws, d0, d1) if r["categoria"].lower() == match.lower()]
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        return
    if not rows:
        await update.message.reply_text(f"📭 Sin gastos en {escape_md(match)} este mes\\.", parse_mode="MarkdownV2")
        return
    total = sum(r["valor"] for r in rows)
    mes_label = hoy.strftime("%B %Y")
    await update.message.reply_text(
        f"🏷️ *{escape_md(match)} — {escape_md(mes_label)}*\n\n"
        f"💰 *Total:* {escape_md(fmt_valor(total))}\n🔢 *Transacciones:* {len(rows)}",
        parse_mode="MarkdownV2"
    )

async def cmd_metodo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args
    if not args:
        mets = escape_md(", ".join(METODOS_PAGO))
        await update.message.reply_text(
            f"Uso: /metodo \\[método\\]\nEjemplo: /metodo nequi\n\nMétodos: {mets}",
            parse_mode="MarkdownV2"
        )
        return
    query_met = " ".join(args).strip()
    match = next((m for m in METODOS_PAGO if m.lower() == query_met.lower()), None)
    if not match:
        await update.message.reply_text(f"❌ Método no reconocido: {query_met}")
        return
    hoy = datetime.today()
    d0 = hoy.replace(day=1).strftime("%d/%m/%Y")
    d1 = hoy.strftime("%d/%m/%Y")
    try:
        ws = get_worksheet()
        rows = [r for r in rows_in_range(ws, d0, d1) if r["metodo_pago"].lower() == match.lower()]
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        return
    if not rows:
        await update.message.reply_text(f"📭 Sin gastos con {escape_md(match)} este mes\\.", parse_mode="MarkdownV2")
        return
    total = sum(r["valor"] for r in rows)
    mes_label = hoy.strftime("%B %Y")
    await update.message.reply_text(
        f"💳 *{escape_md(match)} — {escape_md(mes_label)}*\n\n"
        f"💰 *Total:* {escape_md(fmt_valor(total))}\n🔢 *Transacciones:* {len(rows)}",
        parse_mode="MarkdownV2"
    )

async def cmd_top5(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    hoy = datetime.today()
    d0 = hoy.replace(day=1).strftime("%d/%m/%Y")
    d1 = hoy.strftime("%d/%m/%Y")
    try:
        ws = get_worksheet()
        tops = top_n(ws, d0, d1, 5)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        return
    if not tops:
        await update.message.reply_text("📭 Sin gastos este mes.")
        return
    lines = []
    for i, r in enumerate(tops, 1):
        lines.append(f"{i}\\. {escape_md(fmt_valor(r['valor']))} — {escape_md(r['nota'][:35])} _{escape_md(r['fecha'])}_")
    await update.message.reply_text(
        f"🏆 *Top 5 gastos del mes*\n\n" + "\n".join(lines),
        parse_mode="MarkdownV2"
    )

async def cmd_promedio(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    hoy = datetime.today()
    d0 = hoy.replace(day=1).strftime("%d/%m/%Y")
    d1 = hoy.strftime("%d/%m/%Y")
    try:
        ws = get_worksheet()
        total, count = sum_range(ws, d0, d1)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        return
    if count == 0:
        await update.message.reply_text("📭 Sin gastos este mes.")
        return
    dias = hoy.day
    prom = total / dias
    await update.message.reply_text(
        f"📈 *Promedio diario — {escape_md(hoy.strftime('%B %Y'))}*\n\n"
        f"💰 *Promedio/día:* {escape_md(fmt_valor(prom))}\n"
        f"📅 *Días transcurridos:* {dias}\n"
        f"💵 *Total del mes:* {escape_md(fmt_valor(total))}",
        parse_mode="MarkdownV2"
    )

async def cmd_buscar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args
    if not args:
        await update.message.reply_text("Uso: /buscar \\[texto\\]\nEjemplo: /buscar rappi", parse_mode="MarkdownV2")
        return
    query = " ".join(args).strip()
    try:
        ws = get_worksheet()
        results = search_rows(ws, query)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        return
    if not results:
        await update.message.reply_text(f"📭 Sin resultados para \"{escape_md(query)}\"\\.", parse_mode="MarkdownV2")
        return
    lines = [f"*{escape_md(r['fecha'])}* — {escape_md(fmt_valor(r['valor']))} — {escape_md(r['nota'][:40])}" for r in results]
    await update.message.reply_text(
        f"🔍 *Resultados para \"{escape_md(query)}\"*\n\n" + "\n".join(lines),
        parse_mode="MarkdownV2"
    )

async def cmd_editar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        ws = get_worksheet()
        last_row = find_last_row(ws)
        data = read_row(ws, last_row) if last_row else None
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        return
    if not data:
        await update.message.reply_text("📭 No hay registros para editar.")
        return
    await update.message.reply_text(
        f"✏️ *Editando último registro \\(fila {data['row']}\\)*\n\n" + build_resumen(data)[build_resumen(data).index("\n\n")+2:],
        parse_mode="MarkdownV2",
        reply_markup=kb_post_registro(data["row"])
    )

async def cmd_borrar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        ws = get_worksheet()
        last_row = find_last_row(ws)
        if not last_row:
            await update.message.reply_text("📭 No hay registros.")
            return
        deleted = delete_row_data(last_row)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        return
    await update.message.reply_text(
        f"🗑️ *Registro eliminado*\n\n"
        f"📅 {escape_md(deleted['fecha'])}  💰 {escape_md(fmt_valor(deleted['valor']))}  💳 {escape_md(deleted['metodo_pago'])}\n"
        f"📝 {escape_md(deleted['nota'])}",
        parse_mode="MarkdownV2"
    )

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start",     cmd_ayuda))
    app.add_handler(CommandHandler("ayuda",     cmd_ayuda))
    app.add_handler(CommandHandler("ultimo",    cmd_ultimo))
    app.add_handler(CommandHandler("hoy",       cmd_hoy))
    app.add_handler(CommandHandler("ayer",      cmd_ayer))
    app.add_handler(CommandHandler("semana",    cmd_semana))
    app.add_handler(CommandHandler("mes",       cmd_mes))
    app.add_handler(CommandHandler("quincena",  cmd_quincena))
    app.add_handler(CommandHandler("anual",     cmd_año))
    app.add_handler(CommandHandler("cat",       cmd_cat))
    app.add_handler(CommandHandler("metodo",    cmd_metodo))
    app.add_handler(CommandHandler("top5",      cmd_top5))
    app.add_handler(CommandHandler("promedio",  cmd_promedio))
    app.add_handler(CommandHandler("buscar",    cmd_buscar))
    app.add_handler(CommandHandler("editar",    cmd_editar))
    app.add_handler(CommandHandler("borrar",    cmd_borrar))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.PHOTO,          handle_photo))
    app.add_handler(MessageHandler(filters.Document.IMAGE, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    logger.info("🤖 FinanceBot v4 (Google Sheets) iniciado. Esperando comprobantes...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
