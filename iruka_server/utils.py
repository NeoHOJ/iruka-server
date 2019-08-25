import click
import pprint

class MyClickGroup(click.Group):
    ''' Copied from
    https://github.com/pallets/click/issues/513#issuecomment-504158316
    '''
    def list_commands(self, ctx):
        return self.commands.keys()
