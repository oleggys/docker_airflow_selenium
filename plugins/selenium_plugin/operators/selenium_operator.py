from airflow.models import BaseOperator
from selenium_plugin.hooks.selenium_hook import SeleniumHook


class SeleniumOperator(BaseOperator):
    '''
    Selenium Operator
    '''
    template_fields = ['script_args']


    def __init__(self,
                 script,
                 script_args,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.script = script
        self.script_args = script_args

    def execute(self, context):
        hook = SeleniumHook()
        hook.create_container()
        hook.create_driver()
        try:
            hook.run_script(self.script, self.script_args)
        finally:
            hook.remove_container()
