FROM apache/superset:latest

USER root

# Cài đặt driver Trino mà KHÔNG cho phép nâng cấp SQLAlchemy
# 'trino' chỉ cần thư viện core, không cần cài đè SQLAlchemy mới
RUN pip install --no-cache-dir trino && \
    pip install --no-cache-dir sqlalchemy-trino --no-deps

USER superset