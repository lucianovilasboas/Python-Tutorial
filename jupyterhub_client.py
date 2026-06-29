#!/usr/bin/env python3
"""
JupyterHub Client - Conecta ao servidor JupyterHub e gerencia usuarios/arquivos.

Uso como CLI:
    python jupyterhub_client.py list-users
    python jupyterhub_client.py start-server <user>
    python jupyterhub_client.py upload <user> <local_path> [remote_path]
    python jupyterhub_client.py upload-notebook <user> <notebook_path>
    python jupyterhub_client.py download <user> <remote_path> [local_path]

Uso como biblioteca:
    from jupyterhub_client import JupyterHubClient
    client = JupyterHubClient("http://10.147.20.175", "seu-token")
    client.start_server("usuario1")
    client.upload_file("usuario1", "script.py", "scripts/script.py")
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv
import requests
from tqdm import tqdm
import urllib3

load_dotenv()

logger = logging.getLogger("jupyterhub_client")


class JupyterHubClient:
    """Cliente para interagir com a API REST do JupyterHub e servidores single-user."""

    def __init__(self, hub_url, api_token, verify_ssl=False):
        self.hub_url = hub_url.rstrip("/")
        self.api_token = api_token
        self.verify_ssl = verify_ssl

        if not verify_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self._session = requests.Session()
        self._session.headers["Authorization"] = f"token {self.api_token}"
        self._session.verify = verify_ssl

    def _api_url(self, path):
        return f"{self.hub_url}/hub/api{path}"

    def _user_proxy_url(self, username, path):
        encoded_user = quote(username)
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.hub_url}/user/{encoded_user}/api/contents{path}"

    def _request(self, method, url, **kwargs):
        logger.debug("%s %s", method, url)
        resp = self._session.request(method, url, **kwargs)
        if resp.status_code >= 400:
            try:
                detail = resp.json().get("message", resp.text)
            except (ValueError, KeyError):
                detail = resp.text
            logger.error("HTTP %s %s: %s", resp.status_code, url, detail[:500])
        return resp

    # ------------------------------------------------------------------
    # Usuarios
    # ------------------------------------------------------------------

    def list_users(self):
        """Lista todos os usuarios do JupyterHub."""
        r = self._request("GET", self._api_url("/users"))
        r.raise_for_status()
        return r.json()

    def get_user(self, username):
        """Obtem informacoes de um usuario especifico."""
        r = self._request("GET", self._api_url(f"/users/{quote(username)}"))
        r.raise_for_status()
        return r.json()

    def create_user(self, username):
        """Cria um novo usuario no JupyterHub."""
        r = self._request("POST", self._api_url(f"/users/{quote(username)}"))
        r.raise_for_status()
        return r.json()

    def delete_user(self, username):
        """Remove um usuario do JupyterHub."""
        r = self._request("DELETE", self._api_url(f"/users/{quote(username)}"))
        r.raise_for_status()
        return True

    # ------------------------------------------------------------------
    # Servidores
    # ------------------------------------------------------------------

    def _event_stream(self, url):
        """Generator que produz eventos de um JSON event stream (progress API)."""
        r = self._session.get(url, stream=True, verify=self.verify_ssl)
        r.raise_for_status()
        for line in r.iter_lines(decode_unicode=True):
            if not line:
                continue
            if line.startswith("data:"):
                yield json.loads(line.split(":", 1)[1])

    def server_status(self, username):
        """Retorna True se o servidor do usuario esta rodando, False caso contrario."""
        user = self.get_user(username)
        servers = user.get("servers", {})
        if not servers:
            return False
        default = servers.get("", {})
        return default.get("ready", False)

    def start_server(self, username, server_name="", wait=True, progress_callback=None):
        """Inicia o servidor de um usuario e aguarda ate ficar pronto.

        Retorna a URL de acesso ao servidor.
        """
        user_url = self._api_url(f"/users/{quote(username)}")
        r = self._request("GET", user_url)
        r.raise_for_status()
        user_model = r.json()

        existing = user_model.get("servers", {}).get(server_name)
        if existing and existing.get("ready"):
            logger.info("Servidor de %s ja esta rodando.", username)
            return f"{self.hub_url}{existing['url']}"

        logger.info("Iniciando servidor para %s...", username)
        r = self._request("POST", f"{user_url}/servers/{quote(server_name)}")
        r.raise_for_status()

        if r.status_code == 201:
            logger.info("Servidor de %s foi iniciado e esta pronto.", username)
            r = self._request("GET", user_url)
            r.raise_for_status()
            user_model = r.json()
            server = user_model["servers"][server_name]
            return f"{self.hub_url}{server['url']}"

        if not wait:
            return None

        r = self._request("GET", user_url)
        r.raise_for_status()
        user_model = r.json()
        server = user_model["servers"][server_name]

        progress_url = server.get("progress_url")
        if not progress_url:
            logger.warning("Sem URL de progresso disponivel.")
            return None

        server_url = None
        with tqdm(total=100, desc=f"  {username:>12}  servidor",
                  bar_format="{desc:>16} {percentage:3.0f}% |{bar:20}| {n_fmt:>3}/100",
                  colour="blue", leave=False) as pbar:
            for event in self._event_stream(f"{self.hub_url}{progress_url}"):
                progress = event.get("progress", 0)
                message = event.get("message", "")
                pbar.n = progress
                pbar.refresh()
                if progress_callback:
                    progress_callback(event)
                if event.get("ready"):
                    server_url = f"{self.hub_url}{event['url']}"
                    break

        if server_url is None:
            raise RuntimeError(f"Servidor de {username} nunca ficou pronto.")

        logger.info("Servidor de %s pronto em %s", username, server_url)
        return server_url

    def stop_server(self, username, server_name="", wait=True):
        """Para o servidor de um usuario."""
        user_url = self._api_url(f"/users/{quote(username)}")
        server_url = f"{user_url}/servers/{quote(server_name)}"

        logger.info("Parando servidor de %s...", username)
        r = self._request("DELETE", server_url)
        if r.status_code == 404:
            logger.info("Servidor de %s ja estava parado.", username)
            return True
        r.raise_for_status()

        if r.status_code == 204:
            logger.info("Servidor de %s parado.", username)
            return True

        if not wait:
            return None

        while True:
            r = self._request("GET", user_url)
            r.raise_for_status()
            user_model = r.json()
            if server_name not in user_model.get("servers", {}):
                logger.info("Servidor de %s parado.", username)
                return True
            time.sleep(1)

    def _ensure_server_running(self, username, progress_callback=None):
        """Garante que o servidor do usuario esta rodando, iniciando se necessario."""
        if self.server_status(username):
            return
        self.start_server(username, wait=True, progress_callback=progress_callback)

    # ------------------------------------------------------------------
    # Arquivos (via API do servidor single-user)
    # ------------------------------------------------------------------

    def upload_file(self, username, local_path, remote_path, start_server=True):
        """Envia um arquivo local para o diretorio do usuario no JupyterHub.

        Args:
            username: Nome do usuario destino.
            local_path: Caminho do arquivo local a ser enviado.
            remote_path: Caminho destino no servidor (ex: 'scripts/meu_script.py').
            start_server: Se True, inicia o servidor automaticamente se necessario.
        """
        local = Path(local_path)
        if not local.is_file():
            raise FileNotFoundError(f"Arquivo local nao encontrado: {local_path}")

        if start_server:
            self._ensure_server_running(username)

        remote_path = self._normalize_remote_path(remote_path or local.name)
        content = local.read_bytes()

        return self._put_content(username, remote_path, content, local.name)

    def upload_content(self, username, remote_path, content, content_type="file",
                       content_format="text", start_server=True):
        """Envia conteudo (string ou bytes) como arquivo para o servidor do usuario.

        Args:
            username: Nome do usuario destino.
            remote_path: Caminho destino no servidor.
            content: Conteudo do arquivo (string, bytes ou dict para notebook).
            content_type: 'file' ou 'directory'.
            content_format: 'text', 'base64', ou 'json'.
            start_server: Se True, inicia o servidor se necessario.
        """
        if start_server:
            self._ensure_server_running(username)

        remote_path = self._normalize_remote_path(remote_path)

        if isinstance(content, str):
            content = content.encode("utf-8")
        elif isinstance(content, dict):
            content = json.dumps(content, indent=2, ensure_ascii=False).encode("utf-8")
            content_format = "json"

        if content_format == "base64":
            import base64
            payload = base64.b64encode(content).decode("ascii")
        else:
            payload = content.decode("utf-8") if isinstance(content, bytes) else content

        body = {
            "type": content_type,
            "format": content_format,
            "content": payload,
        }

        url = self._user_proxy_url(username, remote_path)
        r = self._request("PUT", url, json=body)
        r.raise_for_status()
        logger.debug("Arquivo %s enviado para %s", remote_path, username)
        return r.json()

    def upload_notebook(self, username, notebook_path_or_dict, remote_path,
                        start_server=True):
        """Envia um Jupyter Notebook (.ipynb) para o servidor do usuario.

        Args:
            username: Nome do usuario destino.
            notebook_path_or_dict: Caminho do .ipynb local ou dict com o conteudo.
            remote_path: Caminho destino (ex: 'aulas/meu_notebook.ipynb').
            start_server: Se True, inicia o servidor se necessario.
        """
        if isinstance(notebook_path_or_dict, (str, Path)):
            nb_path = Path(notebook_path_or_dict)
            if not nb_path.is_file():
                raise FileNotFoundError(f"Notebook nao encontrado: {notebook_path_or_dict}")
            with open(nb_path, "r", encoding="utf-8") as f:
                notebook = json.load(f)
            if remote_path is None:
                remote_path = nb_path.name
        else:
            notebook = notebook_path_or_dict

        return self.upload_content(username, remote_path, notebook,
                                   content_format="json", start_server=start_server)

    def create_notebook(self, username, remote_path, cells=None,
                        kernel="python3", start_server=True):
        """Cria um notebook vazio ou com celulas pre-definidas.

        Args:
            username: Nome do usuario destino.
            remote_path: Caminho destino (ex: 'novo_notebook.ipynb').
            cells: Lista de celulas no formato [{'cell_type': 'code', 'source': '...'}, ...].
            kernel: Nome do kernel (ex: 'python3').
            start_server: Se True, inicia o servidor se necessario.
        """
        nb = {
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": {
                "kernelspec": {
                    "display_name": f"Python 3 ({kernel})",
                    "language": "python",
                    "name": kernel,
                },
                "language_info": {"name": "python", "version": "3.11.0"},
            },
            "cells": cells or [],
        }
        return self.upload_notebook(username, nb, remote_path, start_server=start_server)

    def download_file(self, username, remote_path, local_path=None,
                      start_server=True):
        """Baixa um arquivo do servidor do usuario.

        Args:
            username: Nome do usuario.
            remote_path: Caminho do arquivo no servidor.
            local_path: Caminho local para salvar (default: nome base do remote_path).
            start_server: Se True, inicia o servidor se necessario.

        Returns:
            bytes do conteudo baixado.
        """
        if start_server:
            self._ensure_server_running(username)

        remote_path = self._normalize_remote_path(remote_path)
        url = self._user_proxy_url(username, remote_path)
        r = self._request("GET", url)
        r.raise_for_status()
        data = r.json()

        content = data.get("content")
        fmt = data.get("format", "text")

        if fmt == "base64":
            import base64
            raw = base64.b64decode(content)
        elif fmt == "json":
            raw = json.dumps(content, indent=2, ensure_ascii=False).encode("utf-8")
        else:
            raw = content.encode("utf-8") if isinstance(content, str) else content

        if local_path:
            Path(local_path).write_bytes(raw)
            logger.info("Arquivo salvo em %s", local_path)

        return raw

    def delete_file(self, username, remote_path, start_server=True):
        """Remove um arquivo ou diretorio do servidor do usuario."""
        if start_server:
            self._ensure_server_running(username)

        remote_path = self._normalize_remote_path(remote_path)
        url = self._user_proxy_url(username, remote_path)
        r = self._request("DELETE", url)
        r.raise_for_status()
        logger.info("%s removido de %s", remote_path, username)
        return True

    def directory_exists(self, username, path, start_server=True):
        """Verifica se um diretorio existe no servidor do usuario."""
        if start_server:
            self._ensure_server_running(username)

        path = self._normalize_relative_path(path)
        url = self._user_proxy_url(username, path)
        r = self._session.get(url, verify=self.verify_ssl)
        if r.status_code == 404:
            return False
        r.raise_for_status()
        return r.json().get("type") == "directory"

    def ensure_directory(self, username, path, start_server=True):
        """Cria o diretorio no servidor se ele nao existir.

        Retorna True se foi necessario criar, False se ja existia.
        """
        if self.directory_exists(username, path, start_server=start_server):
            return False
        self.create_directory(username, path, start_server=start_server)
        return True

    def create_directory(self, username, path, start_server=True):
        """Cria um diretorio no servidor do usuario."""
        return self.upload_content(username, path, "", content_type="directory",
                                   start_server=start_server)

    def upload_directory(self, username, local_dir, remote_base="",
                         pattern="*.ipynb", start_server=True):
        """Envia todos os arquivos que correspondem ao padrao em um diretorio local.

        Args:
            username: Nome do usuario destino.
            local_dir: Caminho do diretorio local.
            remote_base: Caminho base no servidor (ex: 'Tutorial_Python').
            pattern: Glob pattern para filtrar arquivos (ex: '*.ipynb', '*.py', '*').
            start_server: Se True, inicia o servidor automaticamente se necessario.

        Returns:
            dict com 'uploaded', 'failed', 'total'.
        """
        local = Path(local_dir)
        if not local.is_dir():
            raise NotADirectoryError(f"Diretorio local nao encontrado: {local_dir}")

        if start_server:
            self._ensure_server_running(username)

        files = sorted(local.glob(pattern))
        logger.info("Encontrados %d arquivos em %s com padrao '%s'.", len(files), local_dir, pattern)

        if remote_base:
            remote_base = self._normalize_relative_path(remote_base)
            self.ensure_directory(username, remote_base, start_server=False)

        uploaded = []
        failed = []

        desc_fmt = "{:>12}  {:s}".format(username[:12], os.path.basename(local_dir))

        with tqdm(total=len(files), desc=f"  {desc_fmt}",
                  bar_format="{desc:>24} |{bar:24}| {percentage:3.0f}% {n_fmt:>3}/{total_fmt} {unit}",
                  unit="arquivo", colour="green", leave=True) as pbar:
            for file_path in files:
                rel = file_path.relative_to(local).as_posix()
                remote_path = f"{remote_base}/{rel}" if remote_base else rel

                subdir = Path(remote_path).parent.as_posix()
                if subdir and subdir != ".":
                    self.ensure_directory(username, subdir, start_server=False)

                pbar.set_postfix_str(file_path.name)
                try:
                    self.upload_file(username, str(file_path), remote_path, start_server=False)
                    uploaded.append(remote_path)
                except Exception as e:
                    logger.error("Falha ao enviar %s: %s", file_path.name, e)
                    failed.append((str(file_path), str(e)))
                    pbar.colour = "red"
                pbar.update(1)

        result = {"uploaded": uploaded, "failed": failed, "total": len(files)}
        logger.info("Envio concluido: %d enviados, %d falhas de %d total.",
                     len(uploaded), len(failed), len(files))
        return result

    def list_directory(self, username, path="", start_server=True):
        """Lista o conteudo de um diretorio no servidor do usuario."""
        if start_server:
            self._ensure_server_running(username)

        path = self._normalize_relative_path(path)
        url = self._user_proxy_url(username, path)
        r = self._request("GET", url)
        r.raise_for_status()
        data = r.json()
        if data.get("type") == "directory":
            return data.get("content", [])
        raise ValueError(f"'{path}' nao e um diretorio.")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_remote_path(path):
        if not path:
            return "/"
        path = path.replace("\\", "/")
        if not path.startswith("/"):
            path = "/" + path
        return path

    @staticmethod
    def _normalize_relative_path(path):
        path = path.replace("\\", "/")
        if path.startswith("/"):
            path = path[1:]
        return path

    def _put_content(self, username, remote_path, content_bytes, filename):
        import base64
        ext = Path(filename).suffix.lower()
        if ext == ".ipynb":
            fmt = "text"
            payload = content_bytes.decode("utf-8")
        elif ext in (".py", ".txt", ".md", ".csv", ".json", ".yaml", ".yml", ".js", ".html",
                     ".css", ".sh", ".r", ".sql"):
            fmt = "text"
            payload = content_bytes.decode("utf-8")
        else:
            fmt = "base64"
            payload = base64.b64encode(content_bytes).decode("ascii")

        body = {"type": "file", "format": fmt, "content": payload}
        url = self._user_proxy_url(username, remote_path)
        r = self._request("PUT", url, json=body)
        r.raise_for_status()
        logger.debug("Arquivo %s enviado para %s", remote_path, username)
        return r.json()


# ===========================================================================
# CLI
# ===========================================================================

def build_parser():
    parser = argparse.ArgumentParser(
        description="JupyterHub Client - Gerencia usuarios e arquivos no JupyterHub.",
    )
    parser.add_argument(
        "--hub-url",
        default=os.environ.get("JUPYTERHUB_URL", "http://10.147.20.175"),
        help="URL base do JupyterHub (default: $JUPYTERHUB_URL ou http://10.147.20.175)",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("JUPYTERHUB_TOKEN", ""),
        help="Token de API do JupyterHub (default: $JUPYTERHUB_TOKEN)",
    )
    parser.add_argument(
        "--verify-ssl",
        action="store_true",
        default=False,
        help="Verificar certificado SSL (default: False)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Exibir logs detalhados",
    )

    sub = parser.add_subparsers(dest="command", help="Comandos disponiveis")

    # list-users
    sub.add_parser("list-users", help="Listar todos os usuarios")

    # get-user
    p = sub.add_parser("get-user", help="Obter informacoes de um usuario")
    p.add_argument("user", help="Nome do usuario")

    # create-user
    p = sub.add_parser("create-user", help="Criar um novo usuario")
    p.add_argument("user", help="Nome do usuario")

    # delete-user
    p = sub.add_parser("delete-user", help="Remover um usuario")
    p.add_argument("user", help="Nome do usuario")

    # start-server
    p = sub.add_parser("start-server", help="Iniciar servidor de um usuario")
    p.add_argument("user", help="Nome do usuario")

    # stop-server
    p = sub.add_parser("stop-server", help="Parar servidor de um usuario")
    p.add_argument("user", help="Nome do usuario")

    # server-status
    p = sub.add_parser("server-status", help="Verificar status do servidor")
    p.add_argument("user", help="Nome do usuario")

    # upload
    p = sub.add_parser("upload", help="Enviar arquivo local para o servidor")
    p.add_argument("user", help="Nome do usuario destino")
    p.add_argument("local", help="Caminho do arquivo local")
    p.add_argument("remote", nargs="?", default=None, help="Caminho destino (default: nome do arquivo)")
    p.add_argument("--no-start", action="store_true", help="Nao iniciar o servidor automaticamente")

    # upload-notebook
    p = sub.add_parser("upload-notebook", help="Enviar um .ipynb")
    p.add_argument("user", help="Nome do usuario destino")
    p.add_argument("notebook", help="Caminho do .ipynb local")
    p.add_argument("remote", nargs="?", default=None, help="Caminho destino")
    p.add_argument("--no-start", action="store_true", help="Nao iniciar o servidor automaticamente")

    # upload-dir
    p = sub.add_parser("upload-dir", help="Enviar arquivos de um diretorio (ex: todos os *.ipynb)")
    p.add_argument("user", help="Nome do usuario destino")
    p.add_argument("local_dir", help="Caminho do diretorio local")
    p.add_argument("remote", nargs="?", default=None, help="Pasta destino no servidor (default: nome da pasta local)")
    p.add_argument("--pattern", default="*.ipynb", help="Glob pattern (default: *.ipynb)")
    p.add_argument("--no-start", action="store_true", help="Nao iniciar o servidor automaticamente")

    # create-notebook
    p = sub.add_parser("create-notebook", help="Criar um notebook vazio")
    p.add_argument("user", help="Nome do usuario destino")
    p.add_argument("remote", help="Caminho destino (ex: novo.ipynb)")
    p.add_argument("--kernel", default="python3", help="Nome do kernel (default: python3)")
    p.add_argument("--no-start", action="store_true", help="Nao iniciar o servidor automaticamente")

    # download
    p = sub.add_parser("download", help="Baixar arquivo do servidor")
    p.add_argument("user", help="Nome do usuario")
    p.add_argument("remote", help="Caminho do arquivo no servidor")
    p.add_argument("local", nargs="?", default=None, help="Caminho local para salvar")
    p.add_argument("--no-start", action="store_true", help="Nao iniciar o servidor automaticamente")

    # delete-file
    p = sub.add_parser("delete-file", help="Remover arquivo do servidor")
    p.add_argument("user", help="Nome do usuario")
    p.add_argument("remote", help="Caminho do arquivo no servidor")
    p.add_argument("--no-start", action="store_true", help="Nao iniciar o servidor automaticamente")

    # create-dir
    p = sub.add_parser("create-dir", help="Criar diretorio no servidor")
    p.add_argument("user", help="Nome do usuario")
    p.add_argument("path", help="Caminho do diretorio")
    p.add_argument("--no-start", action="store_true", help="Nao iniciar o servidor automaticamente")

    # list-dir
    p = sub.add_parser("list-dir", help="Listar conteudo de um diretorio")
    p.add_argument("user", help="Nome do usuario")
    p.add_argument("path", nargs="?", default="", help="Caminho do diretorio (default: raiz)")
    p.add_argument("--no-start", action="store_true", help="Nao iniciar o servidor automaticamente")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s" if args.verbose else "%(message)s",
        datefmt="%H:%M:%S",
    )

    if not args.token:
        logger.error("Token de API nao informado. Use --token ou defina JUPYTERHUB_TOKEN.")
        sys.exit(1)

    client = JupyterHubClient(args.hub_url, args.token, verify_ssl=args.verify_ssl)
    start = not getattr(args, "no_start", False)

    try:
        if args.command == "list-users":
            users = client.list_users()
            for u in users:
                name = u["name"]
                running = "rodando" if u.get("servers") else "parado"
                admin = " [admin]" if u.get("admin") else ""
                logger.info("  %s (%s)%s", name, running, admin)

        elif args.command == "get-user":
            user = client.get_user(args.user)
            logger.info(json.dumps(user, indent=2, ensure_ascii=False))

        elif args.command == "create-user":
            client.create_user(args.user)
            logger.info("Usuario '%s' criado.", args.user)

        elif args.command == "delete-user":
            client.delete_user(args.user)
            logger.info("Usuario '%s' removido.", args.user)

        elif args.command == "start-server":
            url = client.start_server(args.user)
            logger.info("Servidor pronto: %s", url)

        elif args.command == "stop-server":
            client.stop_server(args.user)
            logger.info("Servidor parado.")

        elif args.command == "server-status":
            status = client.server_status(args.user)
            logger.info("Servidor de %s: %s", args.user, "rodando" if status else "parado")

        elif args.command == "upload":
            client.upload_file(args.user, args.local, args.remote, start_server=start)
            logger.info("Upload concluido.")

        elif args.command == "upload-notebook":
            client.upload_notebook(args.user, args.notebook, args.remote, start_server=start)
            logger.info("Notebook enviado.")

        elif args.command == "upload-dir":
            remote = args.remote or Path(args.local_dir).name
            result = client.upload_directory(args.user, args.local_dir, remote,
                                             pattern=args.pattern, start_server=start)
            for f in result["failed"]:
                logger.info("  FALHA: %s - %s", f[0], f[1])

        elif args.command == "create-notebook":
            client.create_notebook(args.user, args.remote, kernel=args.kernel, start_server=start)
            logger.info("Notebook criado em %s", args.remote)

        elif args.command == "download":
            output = client.download_file(args.user, args.remote, args.local, start_server=start)
            if not args.local:
                logger.info("Conteudo:\n%s", output.decode("utf-8", errors="replace"))

        elif args.command == "delete-file":
            client.delete_file(args.user, args.remote, start_server=start)
            logger.info("Arquivo removido.")

        elif args.command == "create-dir":
            client.create_directory(args.user, args.path, start_server=start)
            logger.info("Diretorio '%s' criado.", args.path)

        elif args.command == "list-dir":
            items = client.list_directory(args.user, args.path, start_server=start)
            for item in items:
                t = "[DIR]" if item.get("type") == "directory" else "[FILE]"
                logger.info("  %s  %s", t, item.get("name", item.get("path", "?")))

        else:
            parser.print_help()

    except requests.exceptions.ConnectionError:
        logger.error("Erro de conexao. Verifique se o JupyterHub esta acessivel em %s", args.hub_url)
        sys.exit(1)
    except requests.exceptions.HTTPError as e:
        logger.error("Erro HTTP: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.error("Erro: %s", e)
        if args.verbose:
            raise
        sys.exit(1)


if __name__ == "__main__":
    main()
