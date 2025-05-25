import pandas as pd
from tikapi import TikAPI, ValidationException, ResponseException
from datetime import datetime
import os
import psycopg2
from dotenv import load_dotenv

# === CARGAR VARIABLES DE ENTORNO ===
load_dotenv()
API_KEY = os.environ["API_KEY"]
DATABASE_URL = os.environ["DATABASE_URL"]
REPORT_NAME = "DB_TENDENCIAS"
COUNTRY_NAME = "ve"

def crear_tabla_si_no_existe():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tendencias_tiktok (
            video_id BIGINT PRIMARY KEY,
            username_id TEXT,
            user_id_tiktok TEXT,
            username TEXT,
            nickname TEXT,
            biography TEXT,
            link_video TEXT,
            caption TEXT,
            length_video INT,
            hashtags TEXT,
            plays BIGINT,
            likes BIGINT,
            shares BIGINT,
            comments BIGINT,
            creation_date TIMESTAMP,
            country TEXT,
            download_date DATE,
            report_name TEXT,
            created_at_bbdd TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()

def convert_id_to_int(df):
    df['video_id'] = pd.to_numeric(df['video_id'], errors='coerce')
    df = df.dropna(subset=['video_id'])
    df['video_id'] = df['video_id'].astype('int64')
    return df

def obtener_datos_tiktok(api, country):
    response = api.public.explore(country=country, count=30)
    data = response.json()

    registros = {
        'username_id': [], 'user_id_tiktok': [], 'username': [], 'nickname': [], 'biography': [],
        'video_id': [], 'link_video': [], 'caption': [], 'length_video': [], 'hashtags': [],
        'plays': [], 'likes': [], 'shares': [], 'comments': [], 'creation_date': [], 'country': []
    }

    for item in data.get('itemList', []):
        author = item.get('author')
        stats = item.get('stats')
        if not author or not stats:
            continue

        registros['username_id'].append(author.get('id'))
        registros['user_id_tiktok'].append(author.get('secUid'))
        registros['username'].append(author.get('uniqueId'))
        registros['nickname'].append(author.get('nickname'))
        registros['biography'].append(author.get('signature'))
        registros['video_id'].append(item.get('id'))
        registros['link_video'].append(f"https://www.tiktok.com/@{author.get('uniqueId')}/video/{item.get('id')}")
        registros['caption'].append(item.get('desc'))
        registros['length_video'].append(item.get('video', {}).get('duration'))
        registros['plays'].append(stats.get('playCount'))
        registros['likes'].append(stats.get('diggCount'))
        registros['shares'].append(stats.get('shareCount'))
        registros['comments'].append(stats.get('commentCount'))
        registros['creation_date'].append(pd.to_datetime(item.get('createTime'), unit='s'))
        registros['country'].append(country)

        hashtags = [h['hashtagName'] for h in item.get('textExtra', []) if h.get('type') == 1]
        registros['hashtags'].append(", ".join(hashtags))

    df = pd.DataFrame(registros)
    df['download_date'] = datetime.now().date()
    df['report_name'] = REPORT_NAME
    df['created_at_bbdd'] = datetime.now()
    return convert_id_to_int(df)

def guardar_datos_en_postgres(df):
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO tendencias_tiktok (
                video_id, username_id, user_id_tiktok, username, nickname, biography,
                link_video, caption, length_video, hashtags,
                plays, likes, shares, comments, creation_date,
                country, download_date, report_name, created_at_bbdd
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (video_id) DO UPDATE SET
                plays = EXCLUDED.plays,
                likes = EXCLUDED.likes,
                shares = EXCLUDED.shares,
                comments = EXCLUDED.comments,
                download_date = EXCLUDED.download_date,
                created_at_bbdd = EXCLUDED.created_at_bbdd;
        """, tuple(row[col] for col in [
            'video_id', 'username_id', 'user_id_tiktok', 'username', 'nickname', 'biography',
            'link_video', 'caption', 'length_video', 'hashtags',
            'plays', 'likes', 'shares', 'comments', 'creation_date',
            'country', 'download_date', 'report_name', 'created_at_bbdd'
        ]))

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    try:
        crear_tabla_si_no_existe()
        api = TikAPI(API_KEY)
        df = obtener_datos_tiktok(api, COUNTRY_NAME)

        if df.empty:
            print("⚠️ No se obtuvieron datos desde TikAPI.")
        else:
            print(f"📊 {len(df)} filas obtenidas.")
            guardar_datos_en_postgres(df)

    except ValidationException as e:
        print(f"❌ Validación fallida: {e}")
    except ResponseException as e:
        print(f"❌ Error de respuesta: {e}")
    except Exception as e:
        print(f"❌ Error inesperado: {str(e)}")
