from fastapi import APIRouter, UploadFile, File, HTTPException

from backend.services.whisper_client import whisper_client

router = APIRouter(prefix="/api/voice", tags=["voice"])

_MAX_AUDIO_SIZE_BYTES = 25 * 1024 * 1024


@router.post("/transcribe")
async def transcribe(file: UploadFile = File(...)):
    """Upload audio file, return transcribed text via OpenAI Whisper."""
    if not file.filename:
        raise HTTPException(400, "No file uploaded")

    audio_bytes = await file.read(_MAX_AUDIO_SIZE_BYTES + 1)
    if len(audio_bytes) == 0:
        raise HTTPException(400, "Empty audio file")
    if len(audio_bytes) > _MAX_AUDIO_SIZE_BYTES:
        raise HTTPException(400, "File too large (max 25MB)")

    try:
        text = await whisper_client.transcribe(audio_bytes, filename=file.filename or "audio.webm")
        return {"text": text}
    except ValueError as e:
        raise HTTPException(500, str(e))
    except Exception as e:
        raise HTTPException(500, f"Transcription failed: {e}")
