import flask
import requests
from data_integration.logging import events
from data_integration import config


class Slack(events.EventHandler):
    node_output: {tuple: [events.Event]} = None
    node_error_output: {tuple: [events.Event]} = None

    def handle_event(self, event: events.Event):
        """
        Slack event handler. Send a notification to a slack channel with respect to the current event.
        Args:
            event: The current event of interest
        """
        if isinstance(event, events.Output):
            key = tuple(event.node_path)

            if event.is_error:
                if not self.node_error_output:
                    self.node_error_output = {}

                if key in self.node_error_output:
                    self.node_error_output[key].append(event)
                else:
                    self.node_error_output[key] = [event]
            else:
                if not self.node_output:
                    self.node_output = {}

                if key in self.node_output:
                    self.node_output[key].append(event)
                else:
                    self.node_output[key] = [event]

        elif isinstance(event, events.NodeFinished):
            key = tuple(event.node_path)
            if not event.succeeded and event.is_pipeline is False:
                self.send_message(
                    message='\n:baby_chick: Ooops, a hiccup in ' +
                            '_ <' + config.base_url() + flask.url_for('data_integration.node_page', path='/'.join(
                        event.node_path)) + ' | ' + '/'.join(event.node_path) + ' > _',
                    # output='\n'.join(self.node_output[key]), error_output='\n'.join(self.node_error_output[key]))
                    output=self.node_output[key], error_output=self.node_error_output[key])

    def format_message(self, message: {} or str or [events.Event]):
        """
        Format a single slack message, when it is under dictionary type (key=format type keyword: value=string to be formatted)
        Args:
            message: The message to be formatted.
        """

        if type(message) is str:
            return message
        elif (type(message) is dict) and len(message) > 0:
            key = str(list(message.keys())[0])
            value = str(list(message.values())[0])
            if type(key) is str:
                if key == 'verbatim' or key == 'error':
                    return '```' + value + '```'
                elif key == 'bold':
                    return '*' + value + '*'
                elif key == 'italics':
                    return '_' + value + '_'
            return value
        elif type(message) is list:
            output, last_format = '', ''
            for event in message:
                if event.format == ('verbatim' or events.Output.Format.VERBATIM):
                    if last_format == event.format:
                        # append new verbatim line to the already initialized verbatim output
                        output = output[0:-3] + '\n' + event.message + '```'
                    else:
                        output += '\n' + '```' + event.message + '```'
                elif event.format == ('italics' or events.Output.Format.ITALICS):
                    output += '\n_ ' + str(event.message).replace('\n', ' ') + ' _ '
                else:
                    output = '\n' + event.message
                last_format = event.format
            return output
        return message

    def send_message(self, message: str, output: events.Event = None, error_output: events.Event = None):
        """
        Sends a notification through a post request to an incoming-webhook channel.
        Args:
            message: The message to be sent
            output: verbatim output as an attachment to the notification
            error_output: error output event as an attachment to the notification
        """

        message = self.format_message(message)

        attachments = []
        if (output):
            attachments.append(
                {'text': self.format_message(output),
                 'mrkdwn_in': ['text']})
        if (error_output):
            attachments.append(
                {'text': self.format_message(error_output),
                 'color': '#eb4d5c',
                 'mrkdwn_in': ['text']})

        response = requests.post('https://hooks.slack.com/services/' + config.slack_token(),
                                 json={'text': message, 'attachments': attachments})

        if response.status_code != 200:
            raise ValueError(
                'Request to slack returned an error %s. The response is:\n%s' % (response.status_code, response.text)
            )
