# 🐬 Transient README

You should have `pipenv` installed. `pyenv` is optional but preferred for installing the recommended version of Python.

```bash
pipenv install
pipenv shell
```

Copy configurations:

```bash
cp server.yml{.example,}
cp iruka_client/iruka.yml{.example,}
```

Generate protobuf bindings/grpc stubs:

```bash
scripts/gen_protos.py
```

Then you are all set.

* For server:
  ```bash
  python iruka_server.py runserver
  ```

* For clients:
  ```bash
  cd iruka_client
  python -m iruka
  ```
