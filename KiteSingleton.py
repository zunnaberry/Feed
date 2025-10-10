from kiteconnect import KiteConnect


class KiteSingleton:
    _instance = None  # Class variable to hold the singleton instance

    def __new__(cls, api_key=None, api_secret=None, request_token=None):
        if cls._instance is None:
            if api_key and api_secret and request_token:
                cls._instance = super(KiteSingleton, cls).__new__(cls)
                cls._instance._initialized = False
                cls._instance.api_key = api_key
                cls._instance.api_secret = api_secret
                cls._instance.request_token = request_token
                cls._instance.kite = None
                cls._instance.access_token = None
                cls._instance._initialize_kite()
                cls._instance._initialized = True
            else:
                raise ValueError("API key, secret, and request token are required for initialization.")
        return cls._instance

    def _initialize_kite(self):
        """Initialize KiteConnect and get access token."""
        try:
            self.kite = KiteConnect(api_key=self.api_key)

            # Generate session to get access token
            session_data = self.kite.generate_session(self.request_token, api_secret=self.api_secret)
            self.access_token = session_data["access_token"]

            # Set the access token for future requests
            self.kite.set_access_token(self.access_token)

            print("Kite connection established with access token:", self.access_token)
        except Exception as e:
            print(f"Error during KiteConnect initialization: {str(e)}")

    def get_kite_instance(self):
        """Return the initialized KiteConnect instance."""
        return self.kite
