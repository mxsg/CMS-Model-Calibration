import os

from datetime import datetime


class ReportBuilder:
    """Instances of this class can be used to build and save markdown reports.

    Markdown generation functionality is inspired by the Python Markdown Generator module from
    https://github.com/cmccandless/markdown-generator
    """

    def __init__(self, base_path=None, filename=None):
        self.report = ''

        if base_path is None:
            base_path = '.'
        self.base_path = base_path

        self.resource_dir = 'figures'

        if filename is None:
            now = datetime.now()
            filename = "report_{}.md".format(now.strftime('%Y-%m-%d_%H-%M-%S'))

        self.filename = filename

    def append(self, content=None):
        """ Append the content to the current report. This adds a newline after the string content. """

        if content is not None:
            self.report += str(content).rstrip() + '\n'

    def append_paragraph(self, content):
        """ Append the content to the current report.
        This adds an empty line before and a newline after the string content.
        """
        self.append('\n')
        self.append(str(content))

    def write(self):
        """Write the contents of the report to the file selected when constructing the report builder."""
        report_path = os.path.join(self.base_path, self.filename)

        with open(report_path, 'w') as f:
            f.write(self.report)


class CodeBlock:
    """A representation for a markdown code block."""

    def __init__(self):
        self.content = []

    def append(self, content):
        self.content.append(content)
        return self

    def __str__(self):
        if not self.content:
            return ''
        else:
            block = []
            block.append('```')
            block.extend(self.content)
            block.append('```')
            return '\n'.join(block)


class ReportingEntity:
    """A mixin to allow a class to log into markdown reports built with a ReportBuilder."""

    def __init__(self, report_builder=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if report_builder is None:
            report_builder = ReportBuilder()
        self.report = report_builder
