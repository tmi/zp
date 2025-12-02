
import fire
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

class AppCLI:
    def serve(self, rs_agg_service_url: str = "http://localhost:3000", host: str = "0.0.0.0", port: int = 3001):
        """
        Starts the FastAPI web server to visualize rs_agg_service output.

        Args:
            rs_agg_service_url: The URL of the rs_agg_service.
            host: The host address to serve the application on.
            port: The port to serve the application on.
        """
        app = FastAPI()
        templates = Jinja2Templates(directory=Path(__file__).parent)

        @app.get("/", response_class=HTMLResponse)
        async def read_root(request: Request):
            return templates.TemplateResponse(
                "index.html", {"request": request, "rs_agg_service_url": rs_agg_service_url}
            )

        uvicorn.run(app, host=host, port=port)

if __name__ == "__main__":
    fire.Fire(AppCLI)
