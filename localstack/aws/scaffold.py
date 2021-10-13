import io
from typing import Dict, List, Set

from botocore import xform_name
from botocore.model import (
    ListShape,
    MapShape,
    OperationModel,
    Shape,
    StringShape,
    StructureShape, ServiceModel,
)
from typing_extensions import OrderedDict

from localstack.aws.spec import load_service
from localstack.utils.common import camel_to_snake_case, snake_to_camel_case


class ShapeNode:
    service: ServiceModel
    shape: Shape

    def __init__(self, service: ServiceModel, shape: Shape) -> None:
        super().__init__()
        self.service = service
        self.shape = shape

    @property
    def is_request(self):
        for operation_name in self.service.operation_names:
            operation = self.service.operation_model(operation_name)
            if self.shape.name == operation.input_shape.name:
                return True

        return False

    @property
    def name(self) -> str:
        return self.shape.name

    @property
    def is_exception(self):
        metadata = self.shape.metadata
        return metadata.get("error") or metadata.get("exception")

    @property
    def is_primitive(self):
        return self.shape.type_name in ["integer", "boolean", "float", "double", "string"]

    @property
    def is_enum(self):
        return isinstance(self.shape, StringShape) and self.shape.enum

    @property
    def dependencies(self) -> List[str]:
        shape = self.shape

        if isinstance(shape, StructureShape):
            return [v.name for v in shape.members.values()]
        if isinstance(shape, ListShape):
            return [shape.member.name]
        if isinstance(shape, MapShape):
            return [shape.key.name, shape.value.name]

        return []

    def print_declaration(self, output):
        code = ""

        shape = self.shape

        if isinstance(shape, StructureShape):
            base = "TypedDict"
            members = ""

            if self.is_exception:
                base = "ServiceException"
            if self.is_request:
                base = "ServiceRequest"

            if not shape.members:
                members = "    pass"

            for k, v in shape.members.items():
                members += f"    {k}: {v.name}\n"

            code = f"""class {shape.name}({base}):\n{members}"""
        elif isinstance(shape, ListShape):
            code = f"{shape.name} = List[{shape.member.name}]\n"
        elif isinstance(shape, MapShape):
            code = f"{shape.name} = Dict[{shape.key.name}, {shape.value.name}]"
        elif isinstance(shape, StringShape):
            if shape.enum:
                code += f"class {shape.name}(str):\n"
                for v in shape.enum:
                    k = v.replace("-", "_")  # TODO: create a proper "enum_value_to_token" function
                    code += f'    {k} = "{v}"\n'
                code += "\n"
            else:
                code = f"{shape.name} = str"
        elif shape.type_name == "string":
            code = f"{shape.name} = str"
        elif shape.type_name == "integer":
            code = f"{shape.name} = int"
        elif shape.type_name == "double":
            code = f"{shape.name} = float"
        elif shape.type_name == "float":
            code = f"{shape.name} = float"
        elif shape.type_name == "boolean":
            code = f"{shape.name} = bool"
        elif shape.type_name == "blob":
            code = f"{shape.name} = bytes"  # FIXME check what type blob really is
        elif shape.type_name == "timestamp":
            code = f"{shape.name} = str"  # FIXME
        else:
            code = f"# unknown shape type for {shape.name}: {shape.type_name}"
        # TODO: BoxedInteger?

        output.write(code)
        output.write("\n")

    def get_order(self):
        """
        Defines a basic order in which to sort the stack of shape nodes before printing.
        First all non-enum primitives are printed, then enums, then exceptions, then all other types.
        """
        if self.is_primitive:
            if self.is_enum:
                return 1
            else:
                return 0

        if self.is_exception:
            return 2

        return 3


def generate_service_types(output, service: ServiceModel):
    output.write("from typing import Dict, List, TypedDict\n")
    output.write("\n")
    output.write("from localstack.aws.api import handler, RequestContext, ServiceException, ServiceRequest")
    output.write("\n")

    # ==================================== print type declarations
    nodes: Dict[str, ShapeNode] = dict()

    for shape_name in service.shape_names:
        shape = service.shape_for(shape_name)
        nodes[shape_name] = ShapeNode(service, shape)

    output.write("__all__ = [\n")
    for name in nodes.keys():
        output.write(f"    \"{name}\",\n")
    output.write("]\n")

    printed: Set[str] = set()
    stack: List[str] = list(nodes.keys())

    stack = sorted(stack, key=lambda name: nodes[name].get_order())
    stack.reverse()

    while stack:
        name = stack.pop()
        if name in printed:
            continue
        node = nodes[name]

        dependencies = [dep for dep in node.dependencies if dep not in printed]

        if not dependencies:
            node.print_declaration(output)
            printed.add(name)
        else:
            stack.append(name)
            stack.extend(dependencies)
            # TODO: circular dependencies (do they exist?)


def generate_service_api(output, service: ServiceModel, doc=True):
    service_name = service.service_name.replace('-', '_')
    class_name = service_name + "_api"
    class_name = snake_to_camel_case(class_name)

    output.write(f"class {class_name}:\n")
    output.write(f"\n")
    output.write(f"    service = \"{service.service_name}\"\n")
    output.write(f"    version = \"{service.api_version}\"\n")
    for op_name in service.operation_names:
        operation: OperationModel = service.operation_model(op_name)

        fn_name = camel_to_snake_case(op_name)
        input_shape = operation.input_shape.name

        if operation.output_shape:
            output_shape = operation.output_shape.name
        else:
            output_shape = "None"

        output.write("\n")
        shape = operation.input_shape
        shape: StructureShape
        members = list(shape.members)
        parameters = OrderedDict()
        param_shapes = OrderedDict()

        for m in shape.required_members:
            members.remove(m)
            m_shape = shape.members[m]
            parameters[xform_name(m)] = m_shape.name
            param_shapes[xform_name(m)] = m_shape
        for m in members:
            m_shape = shape.members[m]
            param_shapes[xform_name(m)] = m_shape
            parameters[xform_name(m)] = f"{m_shape.name} = None"

        param_list = ", ".join([f"{k}: {v}" for k, v in parameters.items()])
        output.write(f"    @handler(\"{operation.name}\")\n")
        output.write(f"    def {fn_name}(self, context: RequestContext, {param_list}) -> {output_shape}:\n")

        # convert html documentation to rst and print it into to the signature
        if doc:
            html = operation.documentation
            import pypandoc

            doc = pypandoc.convert_text(html, "rst", format="html")
            output.write(f'        """\n')
            output.write(f"{doc.strip()}\n")
            output.write("\n")

            # parameters
            for param_name, shape in param_shapes.items():
                # FIXME: this doesn't work properly
                pdoc = pypandoc.convert_text(shape.documentation, "rst", format="html")
                pdoc = pdoc.strip().split(".")[0] + "."
                output.write(f":param {param_name}: {pdoc}\n")

            for error in operation.error_shapes:
                output.write(f":raises: {error.name}\n")

            output.write(f'        """\n')

        output.write(f"        raise NotImplementedError\n")


def main():
    # service = load_service("firehose")
    service = load_service("sqs")
    # service = load_service("comprehend", "2017-11-27")
    # service = load_service("firehose", "2015-08-04")

    output = io.StringIO()
    generate_service_types(output, service)
    generate_service_api(output, service, doc=False)

    model = "model.py"
    api = "api.py"

    "from .model import *"

    code = output.getvalue()

    try:
        # try to format with black
        from black import format_str, FileMode
        res = format_str(code, mode=FileMode())
        print(res)
    except:
        # otherwise just print
        print(code)


if __name__ == "__main__":
    main()
