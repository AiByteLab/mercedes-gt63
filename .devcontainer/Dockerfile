FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    vim \
    wget \
    ca-certificates \
    gnupg \
    lsb-release \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Install Docker CLI and Docker Compose
RUN install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && chmod a+r /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && apt-get update \
    && apt-get install -y docker-ce-cli docker-compose-plugin \
    && rm -rf /var/lib/apt/lists/*

# Install OpenJDK 21 and Maven
RUN apt-get update && apt-get install -y \
    openjdk-21-jdk \
    maven \
    && rm -rf /var/lib/apt/lists/*

# Dynamically detect JAVA_HOME and set it globally
RUN JAVA_HOME_PATH=$(readlink -f /usr/bin/javac | sed "s:/bin/javac::") && \
    echo "JAVA_HOME=${JAVA_HOME_PATH}" >> /etc/environment && \
    echo "PATH=${JAVA_HOME_PATH}/bin:${PATH}" >> /etc/environment

# Set ENV using the detected path (find it first, then set)
RUN echo "export JAVA_HOME=\$(readlink -f /usr/bin/javac | sed 's:/bin/javac::')" > /etc/profile.d/java.sh && \
    echo "export PATH=\${JAVA_HOME}/bin:\${PATH}" >> /etc/profile.d/java.sh && \
    chmod +x /etc/profile.d/java.sh

# Create non-root user
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

# Add user to docker group
RUN groupadd -f docker && usermod -aG docker $USERNAME

# Set up JAVA_HOME for vscode user using the dynamic command
RUN echo 'export JAVA_HOME=$(readlink -f /usr/bin/javac | sed "s:/bin/javac::")' >> /home/$USERNAME/.bashrc && \
    echo 'export PATH="${JAVA_HOME}/bin:${PATH}"' >> /home/$USERNAME/.bashrc && \
    chown $USERNAME:$USERNAME /home/$USERNAME/.bashrc

USER $USERNAME
WORKDIR /workspace
