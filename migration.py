import fdb
import sqlite3
from decimal import Decimal
from io import BytesIO
from PIL import Image

# 🔹 Подключение к Firebird
firebird_config = {
    "dsn": "localhost/3050:/opt/firebird/data/BALI.FDB",
    "user": "sysdba",
    "password": "SYSDBA",
    "charset": "WIN1251"
}

# 🔹 Подключение к SQLite
sqlite_db_path = "collection.db"

FIELDS = [
    "NOM", "NAME", "NOMINAL", "UNIT", "AGE", "COUNTRY", "PERIOD", "MINT",
    "MINTMARK", "TYPES", "SERIES", "METAL", "PROBE", "FORMA", "DIAMETR",
    "THICK", "MASS", "SAFETY", "GURT", "GURTLABEL", "AVREV", "DIFFERENCE",
    "NOTE", "AVERS", "REVERS", "STATUS", "PRICE", "CIRC", "DATEEMIS",
    "NUMCATALOG", "VG", "FINE", "VF", "XF", "UNC", "PROOF", "DATAPAY", "PRICEPAY"
]

FB_TO_SQLITE_MAP = {
    "NAME": "title",
    "NOMINAL": "value",
    "UNIT": "unit",
    "AGE": "year",
    "COUNTRY": "country",
    "PERIOD": "period",
    "MINT": "mint",
    "MINTMARK": "mintmark",
    "TYPES": "type",
    "SERIES": "series",
    "METAL": "material",
    "PROBE": "fineness",
    "FORMA": "shape",
    "DIAMETR": "diameter",
    "THICK": "thickness",
    "MASS": "weight",
    "SAFETY": "grade",
    "GURT": "edge",
    "GURTLABEL": "edgelabel",
    "AVREV": "obvrev",
    "STATUS": "status",
    "PRICE": "price1",
    "CIRC": "mintage",
    "DATEEMIS": "dateemis",
    "NUMCATALOG": "catalognum1",
    "DATAPAY": "paydate",
    "PRICEPAY": "payprice",
    "AVERS": "obverseimg",
    "REVERS": "reverseimg",
    "IMAGE": "image"
}

def save_blob_to_photos(cursor, blob_data, nom, img_type):
    """Сохраняет изображение в таблицу photos и возвращает его ID."""
    if blob_data:
        try:
            cursor.execute("INSERT INTO photos (image, title) VALUES (?, NULL)", (blob_data,))
            img_id = cursor.lastrowid
            print(f"✅ NOM {nom}: {img_type} изображение загружено в SQLite (ID: {img_id})")
            return img_id
        except Exception as e:
            print(f"❌ Ошибка записи {img_type} для NOM {nom}: {e}")
    else:
        print(f"⚠️ NOM {nom}: {img_type} отсутствует")
    return None

def save_preview_to_images(cursor, image_data, nom):
    """Сохраняет превью в таблицу images и возвращает его ID."""
    if image_data:
        try:
            cursor.execute("INSERT INTO images (image) VALUES (?)", (image_data,))
            img_id = cursor.lastrowid
            print(f"✅ NOM {nom}: превью загружено в SQLite (ID: {img_id})")
            return img_id
        except Exception as e:
            print(f"❌ Ошибка записи превью для NOM {nom}: {e}")
    return None

def create_preview(image1=None, image2=None):
    """Создает превью (88x44 для двух изображений, 44x44 для одного)."""
    try:
        if image1 and image2:
            img1 = Image.open(BytesIO(image1)).resize((44, 44), Image.LANCZOS)
            img2 = Image.open(BytesIO(image2)).resize((44, 44), Image.LANCZOS)

            preview = Image.new("RGBA", (88, 44))
            preview.paste(img1, (0, 0))
            preview.paste(img2, (44, 0))
        elif image1:
            preview = Image.open(BytesIO(image1)).resize((44, 44), Image.LANCZOS)
        elif image2:
            preview = Image.open(BytesIO(image2)).resize((44, 44), Image.LANCZOS)
        else:
            return None

        output = BytesIO()
        preview.save(output, format="PNG")
        return output.getvalue()
    except Exception as e:
        print(f"⚠️ Ошибка создания превью: {e}")
        return None

def convert_value(value):
    """Преобразование значений для SQLite."""
    if isinstance(value, Decimal):
        return float(value)
    elif isinstance(value, bytes):
        return value.decode("latin-1").strip()
    elif isinstance(value, str):
        return value.strip()
    return value

def migrate_data():
    try:
        fb_conn = fdb.connect(**firebird_config)
        fb_cursor = fb_conn.cursor()

        sqlite_conn = sqlite3.connect(sqlite_db_path)
        sqlite_cursor = sqlite_conn.cursor()

        fb_cursor.execute(f"SELECT {', '.join(FIELDS)} FROM COINS;")
        rows = fb_cursor.fetchall()

        print(f"📌 Найдено {len(rows)} записей в Firebird.")

        failed_noms = []

        for row in rows:
            row_dict = dict(zip(FIELDS, row))
            sqlite_data = {}

            nom = row_dict["NOM"]
            obverse_img_id = None
            reverse_img_id = None
            preview_img_id = None

            obverse_data = None
            reverse_data = None

            for fb_field, sqlite_field in FB_TO_SQLITE_MAP.items():
                value = row_dict.get(fb_field)

                # 🔹 Обрабатываем изображения
                if fb_field == "AVERS" and isinstance(value, fdb.fbcore.BlobReader):
                    obverse_data = value.read()
                    obverse_img_id = save_blob_to_photos(sqlite_cursor, obverse_data, nom, "AVERS")
                    value.close()
                elif fb_field == "REVERS" and isinstance(value, fdb.fbcore.BlobReader):
                    reverse_data = value.read()
                    reverse_img_id = save_blob_to_photos(sqlite_cursor, reverse_data, nom, "REVERS")
                    value.close()
                else:
                    sqlite_data[sqlite_field] = convert_value(value)

            # 🔹 Создаем превью
            preview_data = create_preview(obverse_data, reverse_data)
            if preview_data:
                preview_img_id = save_preview_to_images(sqlite_cursor, preview_data, nom)

            # 🔹 Проверяем успешность загрузки изображений
            if not obverse_img_id or not reverse_img_id:
                failed_noms.append(nom)

            # 🔹 Записываем ID изображений в таблицу coins
            sqlite_data["obverseimg"] = obverse_img_id
            sqlite_data["reverseimg"] = reverse_img_id
            sqlite_data["image"] = preview_img_id

            columns = ', '.join(sqlite_data.keys())
            placeholders = ', '.join(['?'] * len(sqlite_data))
            sqlite_query = f"INSERT INTO coins ({columns}) VALUES ({placeholders})"
            sqlite_cursor.execute(sqlite_query, list(sqlite_data.values()))

        sqlite_conn.commit()
        print(f"✅ Перенос завершен!")

        # 🔹 Вывод номеров записей, для которых изображения не загрузились
        if failed_noms:
            print("\n⚠️ Записи, из которых не удалось выгрузить изображения:")
            for nom in failed_noms:
                print(f"❌ NOM {nom}")

    except Exception as e:
        print(f"❌ Ошибка: {e}")

    finally:
        fb_cursor.close()
        fb_conn.close()
        sqlite_cursor.close()
        sqlite_conn.close()

if __name__ == "__main__":
    migrate_data()
