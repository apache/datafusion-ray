FROM rayproject/ray:2.37.0.cabc24-py312

RUN sudo apt update && \
    sudo apt install -y curl build-essential

# Intall Rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain stable -y

WORKDIR /home/ray

# install dependencies
COPY requirements-in.txt /home/ray/
RUN python3 -m venv venv && \
    source venv/bin/activate && \
    pip3 install -r requirements-in.txt

# add sources
RUN mkdir /home/ray/src
RUN mkdir /home/ray/datafusion_ray
COPY src /home/ray/src/
COPY datafusion_ray /home/ray/datafusion_ray/
COPY pyproject.toml /home/ray/
COPY Cargo.* /home/ray/
COPY build.rs /home/ray/
COPY README.md /home/ray/

# build datafusion_ray
RUN source venv/bin/activate && \
    source /home/ray/.cargo/env && \
    maturin build --release

FROM rayproject/ray:2.37.0.cabc24-py312
COPY --from=0 /home/ray/target/wheels/*.whl /home/ray/
RUN pip3 install /home/ray/*.whl
