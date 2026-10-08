import boto3
import json
import pandas as pd
from io import BytesIO

BUCKET_NAME = "bucket-itops"

PREFIXO_BRONZE = "bronze/"
CAMINHO_SILVER = "silver/dados_consolidados.csv"

s3 = boto3.client("s3")

def listar_arquivos_bronze():

    resposta = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=PREFIXO_BRONZE
    )

    arquivos = []

    if "Contents" in resposta:

        for objeto in resposta["Contents"]:

            chave = objeto["Key"]

            if chave.endswith(".json"):
                arquivos.append(chave)

    return arquivos

def ler_json(chave):

    resposta = s3.get_object(
        Bucket=BUCKET_NAME,
        Key=chave
    )

    conteudo = resposta["Body"].read()

    return json.loads(conteudo)

arquivos = listar_arquivos_bronze()

dados_pa = []
dados_firewall = []


for arquivo in arquivos:

    dados = ler_json(arquivo)

    timestamp = dados.get("timestamp")


    if "access_points" in dados:

        for ap in dados["access_points"]:

            registro = {
                "timestamp": timestamp,
                "tipo": "PA",

                "ID_antena": ap["ID_antena"],

                "Bytes_Sent": ap["Bytes_Sent"],
                "Bytes_Recv": ap["Bytes_Recv"],

                "Active_conn": ap["Active_conn"],

                "CPU_Usage": ap["CPU_Usage"],
                "RAM_Usage": ap["RAM_Usage"]
            }

            dados_pa.append(registro)

    elif "firewall" in dados:

        fw = dados["firewall"]

        registro = {
            "timestamp": timestamp,
            "tipo": "Firewall",

            "ID_antena": None,

            "Bytes_Sent": fw["Bytes_Sent"],
            "Bytes_Recv": fw["Bytes_Recv"],

            "Active_conn": None,

            "CPU_Usage": fw["CPU_Usage"],
            "RAM_Usage": fw["RAM_Usage"],

            "Active_sessions": fw["Active_sessions"],
            "Dropped_packets": fw["Dropped_packets"],
            "top_blocked_ip": fw["top_blocked_ip"],
            "latency_ms": fw["latency_ms"]
        }

        dados_firewall.append(registro)

df_pa = pd.DataFrame(dados_pa)

df_firewall = pd.DataFrame(dados_firewall)

df_novo = pd.concat(
    [df_pa, df_firewall],
    ignore_index=True
)


if df_novo.empty:

    print("Nenhum dado encontrado na Bronze.")

    exit()

try:

    resposta = s3.get_object(
        Bucket=BUCKET_NAME,
        Key=CAMINHO_SILVER
    )

    df_antigo = pd.read_csv(
        BytesIO(resposta["Body"].read())
    )

except s3.exceptions.NoSuchKey:

    df_antigo = pd.DataFrame()

df = pd.concat(
    [df_antigo, df_novo],
    ignore_index=True
)

colunas_id = [
    "timestamp",
    "tipo",
    "ID_antena"
]

df = df.drop_duplicates(
    subset=colunas_id,
    keep="last"
)


df["timestamp"] = pd.to_datetime(
    df["timestamp"]
)

df = df.sort_values(
    ["tipo", "ID_antena", "timestamp"]
)


df["Bytes_Sent_anterior"] = (
    df.groupby(
        ["tipo", "ID_antena"]
    )["Bytes_Sent"]
    .shift(1)
)


df["Bytes_Recv_anterior"] = (
    df.groupby(
        ["tipo", "ID_antena"]
    )["Bytes_Recv"]
    .shift(1)
)


df["throughput_sent_mbps"] = (
    (df["Bytes_Sent"] - df["Bytes_Sent_anterior"])
    * 8
    / 60
    / 1_000_000
)


df["throughput_recv_mbps"] = (
    (df["Bytes_Recv"] - df["Bytes_Recv_anterior"])
    * 8
    / 60
    / 1_000_000
)


def verificar_status(linha):

    if (
        pd.notna(linha["Active_conn"])
        and linha["Active_conn"] > 40
    ):
        return "alta densidade"

    elif linha["CPU_Usage"] > 80:
        return "gargalo de processamento"

    elif linha["RAM_Usage"] > 75:
        return "OOM"

    else:
        return "normal"


df["status_carga"] = df.apply(
    verificar_status,
    axis=1
)

df_pa_consistencia = (
    df[df["tipo"] == "PA"]
    .groupby("timestamp")["Bytes_Sent"]
    .sum()
    .reset_index(name="Bytes_Sent_PA")
)


df_fw_consistencia = (
    df[df["tipo"] == "Firewall"]
    [["timestamp", "Bytes_Sent"]]
    .rename(
        columns={
            "Bytes_Sent": "Bytes_Sent_Firewall"
        }
    )
)


consistencia = pd.merge(
    df_pa_consistencia,
    df_fw_consistencia,
    on="timestamp",
    how="left"
)


consistencia["diferenca_bytes"] = (
    consistencia["Bytes_Sent_PA"]
    - consistencia["Bytes_Sent_Firewall"]
)


consistencia["diferenca_percentual"] = (
    abs(consistencia["diferenca_bytes"])
    / consistencia["Bytes_Sent_Firewall"]
    * 100
)


df = pd.merge(
    df,
    consistencia,
    on="timestamp",
    how="left"
)

df = df.drop(
    columns=[
        "Bytes_Sent_anterior",
        "Bytes_Recv_anterior"
    ]
)


df["timestamp"] = df["timestamp"].dt.strftime(
    "%Y-%m-%d %H:%M:%S"
)

csv = df.to_csv(
    index=False
).encode("utf-8")


s3.put_object(
    Bucket=BUCKET_NAME,
    Key=CAMINHO_SILVER,
    Body=csv,
    ContentType="text/csv"
)


print("ETL Bronze -> Silver concluído.")

print(
    f"s3://{BUCKET_NAME}/{CAMINHO_SILVER}"
)

print(
    f"Total de registros: {len(df)}"
)
