import flask
import requests

from data_integration import config
from data_integration.logging import events


class Slack(events.EventHandler):
    node_output: {tuple: {bool: [events.Event]}} = None

    def handle_event(self, event: events.Event):
        """
        Slack event handler. Send a notification to a slack channel with respect to the current event.
        Args:
            event: The current event of interest
        """
        if isinstance(event, events.Output):
            key = tuple(event.node_path)

            if not self.node_output:
                self.node_output = {}

            if not key in self.node_output:
                self.node_output[key] = {True: [], False: []}

            self.node_output[key][event.is_error].append(event)


        elif isinstance(event, events.NodeFinished):
            key = tuple(event.node_path)
            if not event.succeeded and event.is_pipeline is False:
                self.send_message(
                    message='\n:baby_chick: Ooops, a hiccup in ' +
                            '_ <' + config.base_url() + flask.url_for('data_integration.node_page', path='/'.join(
                        event.node_path)) + ' | ' + '/'.join(event.node_path) + ' > _',
                    output=self.node_output[key][False], error_output=self.node_output[key][True])

    def format_output(self, output_events: [events.Output]):
        output, last_format = '', ''
        for event in output_events:
            if event.format == events.Output.Format.VERBATIM:
                if last_format == event.format:
                    # append new verbatim line to the already initialized verbatim output
                    output = output[0:-3] + '\n' + event.message + '```'
                else:
                    output += '\n' + '```' + event.message + '```'
            elif event.format == events.Output.Format.ITALICS:
                for line in event.message.splitlines():
                    output += '\n _ ' + str(line) + ' _ '
            else:
                output = '\n' + event.message

            last_format = event.format
        return output


    def send_message(self, message: str, output: events.Event = None, error_output: events.Event = None):
        """
        Sends a notification through a post request to an incoming-webhook channel.
        Args:
            message: The message to be sent
            output: verbatim output as an attachment to the notification
            error_output: error output event as an attachment to the notification
        """

        message = {'text': message, 'attachments': []}

        if (output):
            message['attachments'].append(
                {'text': self.format_output(output),
                 'mrkdwn_in': ['text']})
        if (error_output):
            message['attachments'].append(
                {'text': self.format_output(error_output),
                 'color': '#eb4d5c',
                 'mrkdwn_in': ['text']})

        response = requests.post('https://hooks.slack.com/services/' + config.slack_token(), json=message)

        if response.status_code != 200:
            raise ValueError(
                'Request to slack returned an error %s. The response is:\n%s' % (response.status_code, response.text)
            )
