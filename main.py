import os
import sys
import soundfile as sf
import numpy as np
from flask import Flask, request, jsonify
import torch
from TTS.api import TTS
import uuid
from datetime import datetime
from gcloud_storage_manager import sync_bucket_to_local, upload_file_to_bucket, get_gcloud_json_file
from typing import Optional, Tuple
from functools import wraps
from glob import glob

app = Flask(__name__)

SOURCE_DIR_PATH = "sample_voice"
OUTPUT_DIR_PATH = "output_file"
CREDENTIAL_DIR_PATH = "credential"
BEARER_TOKEN = ""

GCLOUD_SERVICE_ACCOUNT = 'russiannewbot23-6bb070d3edee.json'


# Authentication decorator
def require_bearer_token(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return jsonify({'error': 'No Authorization header'}), 401

        try:
            # Extract token from "Bearer <token>"
            auth_type, token = auth_header.split(' ')
            if auth_type.lower() != 'bearer':
                return jsonify({'error': 'Invalid Authorization header format'}), 401

            if token != BEARER_TOKEN:
                return jsonify({'error': 'Invalid token'}), 401

        except ValueError:
            return jsonify({'error': 'Invalid Authorization header format'}), 401

        return f(*args, **kwargs)

    return decorated


class TTSManager:
    def __init__(self, model_name: str, speakers_dir: str):
        # Configure output directory
        self.output_dir = "output_files"
        self.speakers_dir = speakers_dir
        os.makedirs(self.output_dir, exist_ok=True)

        # Initialize device
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        print(f"Using device: {self.device}")

        # Load TTS model
        self.model = TTS(model_name).to(self.device)

        # Initialize speaker cache
        self.speaker_cache = {}

        self._initialize_default_speaker()

    def _initialize_default_speaker(self):
        mp3_files = glob(os.path.join(self.speakers_dir, "*.mp3"))
        if not mp3_files:
            raise ValueError(f"No AUDIO (mp3) files found in {self.speakers_dir}")

        # Store all speaker filenames in a list (without .mp3 extension)
        self.speaker_list = [os.path.splitext(os.path.basename(f))[0] for f in mp3_files]

        default_speaker_path = mp3_files[0]
        self.default_speaker_path = default_speaker_path
        print(f"Using default speaker: {os.path.basename(default_speaker_path)}")

    def _levenshtein_distance(self, s1, s2):
        """
        Calculate the Levenshtein distance between two strings.
        This measures how many single-character edits are needed to change one string into another.
        """
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)

        if len(s2) == 0:
            return len(s1)

        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        return previous_row[-1]

    def find_most_similar_speaker(self, query):
        if not hasattr(self, 'speaker_list') or not self.speaker_list:
            raise ValueError("Speaker list not initialized")

        # Convert query to lowercase and remove extension if present
        query = os.path.splitext(query.lower())[0]

        # Find the most similar name using Levenshtein distance
        distances = [(name, self._levenshtein_distance(query, name.lower()))
                     for name in self.speaker_list]

        # Sort by distance (lower is more similar) and get the most similar name
        most_similar = min(distances, key=lambda x: x[1])[0]

        return most_similar



    def _load_speaker(self, speaker_path: str) -> Tuple[np.ndarray, int]:
        """Load speaker audio file and return audio data and sample rate"""
        audio, sample_rate = sf.read(speaker_path)
        if len(audio.shape) > 1:
            audio = audio[:, 0]  # Convert stereo to mono if necessary
        return audio, sample_rate

    def generate_speech(self,
                        text: str,
                        speaker_name: Optional[str] = None,
                        language: str = "en") -> Tuple[str, str]:
        """Generate speech and return the output file path"""
        # Generate unique filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        output_filename = f"speech_{timestamp}_{unique_id}.mp3"
        output_path = os.path.join(self.output_dir, output_filename)

        # Get speaker path based on name or use default
        if speaker_name is not None:
            most_similar = self.find_most_similar_speaker(speaker_name) + ".mp3"
            speaker_path = os.path.join(SOURCE_DIR_PATH, most_similar)
            if os.path.exists(speaker_path):
                speaker_path = self.default_speaker_path
        else:
            speaker_path = self.default_speaker_path

        # Generate speech (WAV)
        self.model.tts_to_file(
            text=text,
            speaker_wav=speaker_path,
            language=language,
            file_path=output_path
        )

        return output_path, output_filename

    # def __del__(self):
    #     """Cleanup temporary file on shutdown"""
    #     try:
    #         os.unlink(self.temp_speaker_file.name)
    #     except:
    #         pass


# Initialize TTS manager globally
tts_manager = TTSManager(
    model_name="tts_models/multilingual/multi-dataset/xtts_v2",
    speakers_dir=SOURCE_DIR_PATH  # Directory containing speaker MP3 files
)


@app.route('/generate-speech', methods=['POST'])
@require_bearer_token
def generate_speech():
    try:
        # Get JSON data from request
        data = request.get_json()

        if not data or 'text' not in data:
            return jsonify({'error': 'Missing text in request body'}), 400

        # Extract parameters from request
        text = data['text']
        speaker_name = data.get('speaker')  # Optional speaker name (e.g., "steve_jobs")
        language = data.get('language', 'en')  # Default language

        # Generate speech
        output_path, output_filename = tts_manager.generate_speech(
            text=text,
            speaker_name=speaker_name,
            language=language
        )

        fs_upload_data = upload_file_to_bucket(output_path, GCLOUD_SERVICE_ACCOUNT)
        return jsonify({
            'status': 'success',
            'output_file_url': fs_upload_data,
            'filename': output_filename
        })

    except Exception as e:
        return jsonify({
            'error': str(e),
            'status': 'error'
        }), 500


if __name__ == '__main__':
    try:

        if not os.path.exists(SOURCE_DIR_PATH):
            os.makedirs(SOURCE_DIR_PATH)
        if not os.path.exists(OUTPUT_DIR_PATH):
            os.makedirs(OUTPUT_DIR_PATH)
        if not os.path.exists(CREDENTIAL_DIR_PATH):
            os.makedirs(CREDENTIAL_DIR_PATH)

        if not BEARER_TOKEN:
            BEARER_TOKEN = os.getenv('BEARER_TOKEN')
            if not BEARER_TOKEN:
                raise ValueError("BEARER_TOKEN environment variable is not set")

        gcloud_service_account = get_gcloud_json_file(CREDENTIAL_DIR_PATH)

        sync_bucket_to_local(gcloud_service_account, SOURCE_DIR_PATH, 'source_wav/',
                             delimiter='/')

    except Exception as e:
        # Handle unexpected cases and panic
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    app.run(host='0.0.0.0', port=9000)
