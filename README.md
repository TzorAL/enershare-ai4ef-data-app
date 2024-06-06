## Installation

This project is implemented in [Docker]((https://docs.docker.com/)) providing a complete image for the entire service -- see **Dockerfile** and **docker_compose.yml** file for details regarding installation and requirements/dependencies

The project also includes:
* **Dockerfile** and **docker_compose.yml**: docker files responsible for deploying the respective image, as well as **python_requirements.txt** that contains the pip dependencies required to do so.

```bash
docker compose build
docker compose up -d
```
