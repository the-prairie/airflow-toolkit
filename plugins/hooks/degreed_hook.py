from airflow.hooks.http_hook import HttpHook


class DegreedHook(HttpHook):

    def __init__(self, degreed_conn_id):
        self.degreed_token = None
        conn_id = self.get_connection(degreed_conn_id)

        if conn_id.extra_dejson.get('access_token'):
            self.degreed_token = conn_id.extra_dejson.get('access_token')
        super().__init__(method='GET', http_conn_id=degreed_conn_id)

    def get_conn(self, headers):
        """
        Accepts both Basic and Token Authentication.
        If a token exists in the "Extras" section
        with key "token", it is passed in the header.
        If not, the hook looks for a user name and password.
        
        """
        if self.degreed_token:
            headers = {'Authorization': 'Bearer {0}'.format(self.degreed_token)}
            session = super().get_conn(headers)
            session.auth = None
            return session
        return super().get_conn(headers)
