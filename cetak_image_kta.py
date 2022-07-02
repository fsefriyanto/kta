import time

import mysql.connector
import requests

from dotenv import dotenv_values

config = dotenv_values(".env")

db = mysql.connector.connect(
    host=config.get("DB_HOST"),
    user=config.get("DB_USER"),
    passwd=config.get("DB_PASS"),
    database=config.get("DB_NAME")
)


def cetak_image_kta_batch():
    limit = input("Masukkan limit : ")
    cursor = db.cursor()
    sql = f"SELECT _id, desa, nik, jenis_kelamin, kecamatan, kota, m_user_id, nama, id_anggota, provinsi, createdAt, tanggal_lahir " \
          f"FROM Profile " \
          f"WHERE ctk_kta is null " \
          f"AND id_anggota is not null " \
          f"AND status_verifikasi = 1 " \
          f"AND status_aktif = 1 " \
          f"LIMIT {limit}"
    cursor.execute(sql)

    results = cursor.fetchall()

    for data in results:
        bulan = {
            'Jan': 'Jan',
            'Feb': 'Feb',
            'Mar': 'Mar',
            'Apr': 'Apr',
            'May': 'Mei',
            'Jun': 'Jun',
            'Jul': 'Jul',
            'Aug': 'Agu',
            'Sep': 'Sep',
            'Oct': 'Okt',
            'Nov': 'Nop',
            'Dec': 'Des',
        }

        tgl_lahir = data[11].strftime("%d")
        bulan_lahir = bulan[data[11].strftime("%b")]
        tahun_lahir = data[11].strftime("%Y")
        tanggal_lahir = f"{tgl_lahir} {bulan_lahir} {tahun_lahir}"

        jen_kel = {
            "L": "LAKI_LAKI",
            "P": "PEREMPUAN",
        }

        payload = {
            "_id": data[0],
            "desa": data[1],
            "filepath": "/fotokta/" + data[2] + ".png",
            "j_k": jen_kel[data[3]],
            "kec": data[4],
            "kota": data[5],
            "m_user_id": data[6],
            "nama": data[7],
            "nik": data[2],
            "no_kta": data[8],
            "provinsi": data[9],
            "tgl_daftar": data[10].strftime("%Y-%m-%d"),
            "tgl_lahir": tanggal_lahir
        }

        print(payload)

        cetak_image_kta(payload)


def cetak_image_kta(payload):
    url = config.get("BASE_API") + "ktaapi"

    headers = {
        'apikey': config.get("API_KEY"),
        'x-tag': config.get("X_TAG")
    }

    with requests.session() as session:
        with session.post(url, headers=headers, data=payload) as response:
            data = response.text
            print(data)

            if response.status_code != 200:
                print("FAILURE::{0}".format(url))
            return data


def start_process():
    start_time = time.time()
    cetak_image_kta_batch()
    print("--- %s seconds ---" % (time.time() - start_time))
    print("DONE")


if __name__ == "__main__":
    start_process()
