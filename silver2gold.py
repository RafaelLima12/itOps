import boto3
import pandas as pd
from io import BytesIO

BUCKET_NAME = "bucket-itops"

s3 = boto3.client("s3")

obj = s3.get_object(
    Bucket=BUCKET_NAME,
    Key="silver/dados_consolidados.csv"
)

dados = pd.read_csv(BytesIO(obj["Body"].read()))

dados["timestamp"] = pd.to_datetime(dados["timestamp"])

aps = dados[dados["tipo"] == "PA"].copy()

zonas_mortas = (
    aps.groupby("ID_antena")
    .agg(
        trafego_medio_mbps=("throughput_sent_mbps", "mean"),
        conexoes_medias=("Active_conn", "mean"),
        cpu_media=("CPU_Usage", "mean"),
        ram_media=("RAM_Usage", "mean")
    )
    .reset_index()
)

zonas_mortas = zonas_mortas.sort_values(
    "trafego_medio_mbps"
)

limite = zonas_mortas["trafego_medio_mbps"].quantile(0.25)

zonas_mortas["classificacao"] = zonas_mortas[
    "trafego_medio_mbps"
].apply(
    lambda x: "Subutilizada" if x <= limite else "Normal"
)

s3.put_object(
    Bucket=BUCKET_NAME,
    Key="gold/zonas_mortas.csv",
    Body=zonas_mortas.to_csv(index=False),
    ContentType="text/csv"
)

hora_final = aps["timestamp"].max()
hora_inicial = hora_final - pd.Timedelta(hours=5)

ultimas_5h = aps[
    aps["timestamp"] >= hora_inicial
].copy()

previsao = []

LIMITE_CONEXOES = 60

for antena, grupo in ultimas_5h.groupby("ID_antena"):

    grupo = grupo.sort_values("timestamp")

    primeira = grupo.iloc[0]
    ultima = grupo.iloc[-1]

    tempo_horas = (
        ultima["timestamp"] - primeira["timestamp"]
    ).total_seconds() / 3600

    crescimento = (
        ultima["Active_conn"] - primeira["Active_conn"]
    )

    if tempo_horas > 0:
        crescimento_por_hora = crescimento / tempo_horas
    else:
        crescimento_por_hora = 0

    conexoes_atuais = ultima["Active_conn"]

    if crescimento_por_hora > 0:
        horas_ate_limite = (
            LIMITE_CONEXOES - conexoes_atuais
        ) / crescimento_por_hora

        horario_previsto = (
            ultima["timestamp"]
            + pd.Timedelta(hours=horas_ate_limite)
        )
    else:
        horas_ate_limite = None
        horario_previsto = None

    previsao.append({
        "ID_antena": antena,
        "conexoes_atuais": conexoes_atuais,
        "crescimento_medio_conexoes_hora": round(
            crescimento_por_hora, 2
        ),
        "limite_conexoes": LIMITE_CONEXOES,
        "horas_ate_limite": (
            round(horas_ate_limite, 2)
            if horas_ate_limite is not None
            else None
        ),
        "horario_previsto_sobrecarga": horario_previsto
    })

previsao = pd.DataFrame(previsao)

s3.put_object(
    Bucket=BUCKET_NAME,
    Key="gold/previsao_sobrecarga.csv",
    Body=previsao.to_csv(index=False),
    ContentType="text/csv"
)

eficiencia = (
    aps.groupby("ID_antena")
    .agg(
        trafego_medio_mbps=("throughput_sent_mbps", "mean"),
        cpu_media=("CPU_Usage", "mean")
    )
    .reset_index()
)

eficiencia["eficiencia_trafego_cpu"] = (
    eficiencia["trafego_medio_mbps"]
    / eficiencia["cpu_media"].replace(0, pd.NA)
)

eficiencia = eficiencia.sort_values(
    "eficiencia_trafego_cpu",
    ascending=False
)

s3.put_object(
    Bucket=BUCKET_NAME,
    Key="gold/eficiencia_hardware.csv",
    Body=eficiencia.to_csv(index=False),
    ContentType="text/csv"
)


print("ETL Silver -> Gold concluído!")
print()
print("Relatórios gerados:")
print(" - gold/zonas_mortas.csv")
print(" - gold/previsao_sobrecarga.csv")
print(" - gold/eficiencia_hardware.csv")
