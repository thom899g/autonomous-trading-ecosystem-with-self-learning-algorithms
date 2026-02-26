"""
Firebase Configuration Manager
Architectural Rationale: Firebase Firestore provides real-time synchronization,
automatic scaling, and built-in security rules, making it ideal for distributed
agent state management and audit logging.
"""
import os
import json
from typing import Optional, Dict, Any
from dataclasses import dataclass, asdict
import logging

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import Client as FirestoreClient
from google.cloud.firestore_v1.base_query import FieldFilter

logger = logging.getLogger(__name__)

@dataclass
class FirebaseConfig:
    """Immutable configuration for Firebase connection"""
    project_id: str
    private_key: str
    client_email: str
    database_url: Optional[str] = None

class FirebaseManager:
    """Singleton manager for Firebase Firestore connections"""
    _instance: Optional['FirebaseManager'] = None
    _client: Optional[FirestoreClient] = None
    _initialized: bool = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def initialize(self, config: FirebaseConfig) -> None:
        """Initialize Firebase connection with error handling"""
        if self._initialized:
            logger.warning("Firebase already initialized")
            return
            
        try:
            # Validate environment variables
            if not all([config.project_id, config.private_key, config.client_email]):
                raise ValueError("Missing Firebase configuration values")
            
            # Clean private key formatting
            private_key = config.private_key.replace('\\n', '\n')
            
            # Create credentials dictionary
            cred_dict = {
                "type": "service_account",
                "project_id": config.project_id,
                "private_key": private_key,
                "client_email": config.client_email,
                "token_uri": "https://oauth2.googleapis.com/token"
            }
            
            # Initialize Firebase app
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(
                cred, 
                {'databaseURL': config.database_url} if config.database_url else {}
            )
            
            self._client = firestore.client()
            self._initialized = True
            logger.info(f"Firebase initialized for project: {config.project_id}")
            
        except ValueError as ve:
            logger.error(f"Configuration error: {ve}")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize Firebase: {e}")
            raise RuntimeError(f"Firebase initialization failed: {e}")
    
    @property
    def client(self) -> FirestoreClient:
        """Get Firestore client with lazy initialization"""
        if not self._initialized or self._client is None:
            # Attempt auto-initialization from environment
            try:
                config = FirebaseConfig(
                    project_id=os.getenv('FIREBASE_PROJECT_ID', ''),
                    private_key=os.getenv('FIREBASE_PRIVATE_KEY', ''),
                    client_email=os.getenv('FIREBASE_CLIENT_EMAIL', ''),
                    database_url=os.getenv('FIREBASE_DATABASE_URL')
                )
                self.initialize(config)
            except Exception as e:
                logger.error("Auto-initialization failed. Call initialize() first.")
                raise RuntimeError("Firebase not initialized") from e
        return self._client
    
    def get_collection(self, collection_name: str):
        """Get Firestore collection reference with validation"""
        if not collection_name or not isinstance(collection_name, str):
            raise ValueError("Collection name must be a non-empty string")
        return self.client.collection(collection_name)
    
    def write_state(self, agent_id: str, state: Dict[str, Any]) -> None:
        """Write agent state to Firestore with timestamp"""
        try:
            state_doc = {
                **state,
                'timestamp': firestore.SERVER_TIMESTAMP,
                'agent_id': agent_id
            }
            self.get_collection('agent_states').document(agent_id).set(
                state_doc, merge=True
            )
            logger.debug(f"State written for agent: {agent_id}")
        except Exception as e:
            logger.error(f"Failed to write state for agent {agent_id}: {e}")
            raise
    
    def read_state(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """Read agent state from Firestore"""
        try:
            doc = self.get_collection('agent_states').document(agent_id).get()
            return doc.to_dict() if doc.exists else None
        except Exception as e:
            logger.error(f"Failed to read state for agent {agent_id}: {e}")
            return None

# Global instance for easy import
firebase_manager = FirebaseManager()