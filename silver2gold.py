import boto3
import pandas as pd
import io
import numpy as np

BUCKET_NAME = "bucket-itops"

SILVER_CSV = "silver/dados_consolidados.csv"

s3 = boto3.client("s3")


# ==================================================
# LER SILVER
# ==================================================

objeto = s3.get_object(
    Bucket=BUCKET_NAME,
    Key=SILVER_CSV
)

df = pd.read_csv(
    io.BytesIO(objeto["Body"].read())
)

df["timestamp"] = pd.to_datetime(
    df["timestamp"]
)

df_pa = df[
    df["tipo"] == "PA"
].copy()

df_firewall = df[
    df["tipo"] == "Firewall"
].copy()


# ==================================================
# FUNÇÃO PARA SALVAR CSV NO GOLD
# ==================================================

def salvar_gold(dataframe, nome):

    buffer = io.StringIO()

    dataframe.to_csv(
        buffer,
        index=False
    )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"gold/{nome}",
        Body=buffer.getvalue(),
        ContentType="text/csv"
    )

    print(f"Gold criado: gold/{nome}")


# ==================================================
# 1. ZONAS SUBUTILIZADAS
# ==================================================

zonas = (
    df_pa
    .groupby("ID_antena")
    .agg(
        media_trafego_mbps=(
            "throughput_sent_mbps",
            "mean"
        ),

        total_trafego_bytes=(
            "Bytes_Sent",
            "max"
        ),

        media_conexoes=(
            "Active_conn",
            "mean"
        ),

        media_cpu=(
            "CPU_Usage",
            "mean"
        )
    )
    .reset_index()
)

zonas = zonas.sort_values(
    "media_trafego_mbps"
)

zonas["ranking_trafego"] = range(
    1,
    len(zonas) + 1
)

# Menor volume de tráfego
zonas["subutilizada"] = (
    zonas["ranking_trafego"]
    <= max(
        1,
        int(len(zonas) * 0.25)
    )
)

salvar_gold(
    zonas,
    "zonas_subutilizadas.csv"
)


# ==================================================
# 2. PREDIÇÃO DE SOBRECARGA
# ==================================================

LIMITE_CONEXOES = 60

previsoes = []


for antena, grupo in df_pa.groupby(
    "ID_antena"
):

    grupo = grupo.sort_values(
        "timestamp"
    ).copy()

    # Crescimento entre medições
    grupo["crescimento_conexoes"] = (
        grupo["Active_conn"].diff()
    )

    # Média das últimas 5 horas
    crescimento_medio = (
        grupo["crescimento_conexoes"]
        .tail(300)
        .mean()
    )

    atual = grupo.iloc[-1]

    conexoes_atuais = (
        atual["Active_conn"]
    )

    if (
        crescimento_medio > 0
        and conexoes_atuais < LIMITE_CONEXOES
    ):

        minutos = (
            LIMITE_CONEXOES
            - conexoes_atuais
        ) / crescimento_medio

        horario = (
            atual["timestamp"]
            + pd.Timedelta(
                minutes=float(minutos)
            )
        )

    else:

        minutos = np.nan

        horario = pd.NaT

    previsoes.append({

        "ID_antena": antena,

        "timestamp_atual":
            atual["timestamp"],

        "conexoes_atuais":
            conexoes_atuais,

        "limite_conexoes":
            LIMITE_CONEXOES,

        "crescimento_medio_5h":
            crescimento_medio,

        "minutos_ate_limite":
            minutos,

        "horario_estimado_sobrecarga":
            horario
    })


previsao = pd.DataFrame(
    previsoes
)

salvar_gold(
    previsao,
    "previsao_sobrecarga.csv"
)


# ==================================================
# 3. EFICIÊNCIA DE HARDWARE
# ==================================================

eficiencia = (
    df_pa
    .groupby("ID_antena")
    .agg(

        trafego_medio_mbps=(
            "throughput_sent_mbps",
            "mean"
        ),

        cpu_medio=(
            "CPU_Usage",
            "mean"
        )
    )
    .reset_index()
)


eficiencia["eficiencia"] = (
    eficiencia["trafego_medio_mbps"]
    /
    eficiencia["cpu_medio"].replace(
        0,
        np.nan
    )
)


eficiencia = eficiencia.sort_values(
    "eficiencia",
    ascending=False
)

eficiencia["ranking"] = range(
    1,
    len(eficiencia) + 1
)


salvar_gold(
    eficiencia,
    "eficiencia_hardware.csv"
)


# ==================================================
# 4. ANÁLISE DE SEGURANÇA
# ==================================================

df_firewall["hora"] = (
    df_firewall["timestamp"]
    .dt.floor("h")
)


seguranca = (
    df_firewall
    .groupby("hora")
    .agg(

        pacotes_bloqueados=(
            "Dropped_packets",
            "sum"
        ),

        media_cpu=(
            "CPU_Usage",
            "mean"
        ),

        sessoes_ativas=(
            "Active_sessions",
            "mean"
        )
    )
    .reset_index()
)


quantidade_medicoes = (
    df_firewall
    .groupby("hora")
    .size()
    .values
)


seguranca[
    "media_bloqueios_por_minuto"
] = (
    seguranca["pacotes_bloqueados"]
    /
    quantidade_medicoes
)


salvar_gold(
    seguranca,
    "analise_seguranca.csv"
)


# ==================================================
# 5. MAPA DE TRÁFEGO
# ==================================================

trafego_pa = (
    df_pa[
        [
            "timestamp",
            "ID_antena",
            "throughput_sent_mbps"
        ]
    ]
    .rename(
        columns={
            "throughput_sent_mbps":
                "trafego_antena_mbps"
        }
    )
)


trafego_firewall = (
    df_firewall[
        [
            "timestamp",
            "throughput_sent_mbps"
        ]
    ]
    .rename(
        columns={
            "throughput_sent_mbps":
                "trafego_firewall_mbps"
        }
    )
)


mapa = trafego_pa.merge(
    trafego_firewall,
    on="timestamp",
    how="inner"
)


mapa["percentual_do_firewall"] = (
    mapa["trafego_antena_mbps"]
    /
    mapa["trafego_firewall_mbps"].replace(
        0,
        np.nan
    )
) * 100


salvar_gold(
    mapa,
    "mapa_trafego.csv"
)


# ==================================================
# 6. GARGALO DE SAÍDA
# ==================================================

gargalo = (
    df_firewall[
        [
            "timestamp",
            "CPU_Usage",
            "latency_ms",
            "Active_sessions"
        ]
    ]
    .copy()
)


correlacao = (
    df_firewall[
        [
            "CPU_Usage",
            "latency_ms"
        ]
    ]
    .corr()
    .iloc[0, 1]
)


gargalo[
    "correlacao_cpu_latencia"
] = correlacao


gargalo["possivel_gargalo"] = (
    (gargalo["CPU_Usage"] > 80)
    &
    (gargalo["latency_ms"] > 100)
)


salvar_gold(
    gargalo,
    "gargalo_saida.csv"
)


print(
    "\nProcesso Silver -> Gold finalizado!"
)