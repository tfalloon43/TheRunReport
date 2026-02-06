let paddleLoader;

const PADDLE_SCRIPT_URL = "https://cdn.paddle.com/paddle/v2/paddle.js";

export function loadPaddle() {
  if (typeof window === "undefined") return Promise.reject(new Error("No window"));
  if (window.Paddle) return Promise.resolve(window.Paddle);
  if (paddleLoader) return paddleLoader;

  paddleLoader = new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = PADDLE_SCRIPT_URL;
    script.async = true;
    script.onload = () => {
      if (window.Paddle) resolve(window.Paddle);
      else reject(new Error("Paddle failed to load."));
    };
    script.onerror = () => reject(new Error("Paddle script failed to load."));
    document.body.appendChild(script);
  });

  return paddleLoader;
}

export async function initPaddle() {
  const paddle = await loadPaddle();
  const token = import.meta.env.VITE_PADDLE_CLIENT_TOKEN;
  if (!token) {
    throw new Error("Missing VITE_PADDLE_CLIENT_TOKEN.");
  }

  if (paddle.Environment && paddle.Environment.set) {
    paddle.Environment.set("sandbox");
  }
  paddle.Initialize({ token });

  return paddle;
}
