FROM debian:latest

# Atualiza os pacotes e instala dependências
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    gnupg \
    python3 \
    python3-pip \
    python3-venv \
    libx11-xcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxi6 \
    libxtst6 \
    libnss3 \
    libgbm1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    xvfb \
    xdotool \
    && rm -rf /var/lib/apt/lists/*


# Cria um usuário não-root para rodar de forma segura
RUN useradd -m chrome && \
    mkdir -p /home/chrome/.cache && \
    chown -R chrome:chrome /home/chrome

# Define o diretório de trabalho
WORKDIR /app

# Copia todos os arquivos do diretório local para o container
COPY . /app

# Corrige permissões para evitar erro de escrita nos logs
RUN chown -R chrome:chrome /app && chmod -R 777 /app

# Cria e ativa um ambiente virtual Python
RUN python3 -m venv /app/venv && \
    /app/venv/bin/pip install --no-cache-dir --upgrade pip && \
    /app/venv/bin/pip install --no-cache-dir -r requirements.txt

# Adiciona o venv ao PATH para que o Python e o pip do ambiente virtual sejam usados
ENV PATH="/app/venv/bin:$PATH"

# Muda para o usuário "chrome"
USER chrome

# Mantém o container rodando sem executar nada automaticamente
CMD ["bash"]