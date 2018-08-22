import os

from datetime import datetime


class ReportBuilder:
    """Instances of this class can be used to build and save markdown reports.

    Markdown generation functionality is inspired by the Python Markdown Generator module from
    https://github.com/cmccandless/markdown-generator
    """

    def __init__(self, base_path=None, filename=None):
        self.report = ''
        self.figures = {}

        if base_path is None:
            base_path = '.'
        self.base_path = base_path

        self.resource_dir = 'figures'
        self.resource_path = os.path.join(self.base_path, self.resource_dir)

        # Todo Check whether these formats are supported by matplotlib
        self.image_formats = ['pdf', 'png', 'eps']
        self.inline_image_format = 'png'

        if filename is None:
            now = datetime.now()
            filename = "report_{}.md".format(now.strftime('%Y-%m-%d_%H-%M-%S'))

        self.filename = filename

    # Todo Convert into property?
    def set_base_path(self, path):
        self.base_path = path
        self.resource_path = os.path.join(self.base_path, self.resource_dir)

    def get_base_path(self):
        return self.base_path

    def append(self, content=None):
        """ Append the content to the current report. This adds a newline after the string content. """

        if content is None:
            self.report += '\n'
        else:
            self.report += str(content) + '\n'

    def append_paragraph(self, content):
        """ Append the content to the current report.
        This adds an empty line before and a newline after the string content.
        """
        self.append()
        self.append(str(content))

    def write(self):
        """Write the contents of the report to the file selected when constructing the report builder."""
        report_path = os.path.join(self.base_path, self.filename)

        with open(report_path, 'w') as f:
            f.write(self.report)

        # Write figures to files
        os.makedirs(self.resource_path, exist_ok=True)

        for figure_id, (fig, ax) in self.figures.items():

            for image_format in self.image_formats:
                fig.savefig(os.path.join(self.resource_path, figure_id + '.' + image_format))

    def add_figure(self, fig, axes, identifier, tight_layout=True):

        if tight_layout:
            fig.tight_layout()

        # Todo Maybe refactor into its own class?
        if identifier in self.figures:
            raise ValueError("Figure with identical identifier '{}' already included in report!".format(identifier))

        self.figures[identifier] = (fig, axes)

        inline_figure_path = os.path.join(self.resource_dir, identifier + '.' + self.inline_image_format)
        self.append('![Figure {}]({})'.format(identifier, inline_figure_path))
        self.append()

        image_links = []
        for image_format in self.image_formats:
            figure_path = os.path.join(self.resource_dir, identifier + '.' + image_format)

            image_link = '[{}]({})'.format(image_format, figure_path)
            image_links.append(image_link)

        self.append("Figure in other image formats: " + ", ".join(image_links))
        self.append()


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


class Figure:
    """A representation of a figure."""

    def __init__(self, fig, axes, title=''):
        pass


class ReportingEntity:
    """A mixin to allow a class to log into markdown reports built with a ReportBuilder."""

    def __init__(self, report_builder=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if report_builder is None:
            report_builder = ReportBuilder()
        self.report = report_builder
