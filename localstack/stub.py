import json
import os
import pkgutil
from typing import Dict, List, Set

from botocore import xform_name
from botocore.model import (
    ListShape,
    MapShape,
    OperationModel,
    ServiceModel,
    Shape,
    StringShape,
    StructureShape,
)
from typing_extensions import OrderedDict

from localstack.utils.common import camel_to_snake_case, first_char_to_upper


def load_service(service: str, version: str) -> ServiceModel:
    """
    For example: load_service("sqs", "2012-11-05")
    """
    path = os.path.join("data", service, version, "service-2.json")
    data = pkgutil.get_data("botocore", path)
    service_description = json.loads(data)

    return ServiceModel(service_description, service)


class ShapeNode:
    shape: Shape

    def __init__(self, shape: Shape) -> None:
        super().__init__()
        self.shape = shape

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
    def dependencies(self) -> List[str]:
        shape = self.shape

        if isinstance(shape, StructureShape):
            return [v.name for v in shape.members.values()]
        if isinstance(shape, ListShape):
            return [shape.member.name]
        if isinstance(shape, MapShape):
            return [shape.key.name, shape.value.name]

        return []

    def print_declaration(self):
        code = ""

        shape = self.shape

        if isinstance(shape, StructureShape):
            base = "TypedDict"
            members = ""

            if self.is_exception:
                base = "Exception"

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
                code += f"class {shape.name}(Enum):\n"
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
            code = f"{shape.name} = bytes"
        elif shape.type_name == "timestamp":
            code = f"{shape.name} = str"  # FIXME
        else:
            code = f"# unknown shape type for {shape.name}: {shape.type_name}"

        print(code)
        print()


def main():
    service = load_service("sqs", "2012-11-05")
    # service = load_service("comprehend", "2017-11-27")
    # service = load_service("firehose", "2015-08-04")

    code = ""
    code += "from enum import Enum\n"
    code += "from typing import Dict, List, TypedDict\n"
    code += "\n"
    print(code)

    # ==================================== print type declarations
    nodes: Dict[str, ShapeNode] = dict()

    for shape_name in service.shape_names:
        shape = service.shape_for(shape_name)
        nodes[shape_name] = ShapeNode(shape)

    printed: Set[str] = set()
    stack: List[str] = list(nodes.keys())

    # then try the rest
    stack.reverse()  # for alphabetical order
    while stack:
        name = stack.pop()
        if name in printed:
            continue
        node = nodes[name]

        dependencies = [dep for dep in node.dependencies if dep not in printed]

        if not dependencies:
            node.print_declaration()
            printed.add(name)
        else:
            stack.append(name)
            stack.extend(dependencies)
            # TODO: circular dependencies

    # ================================================= print skeleton

    class_name = first_char_to_upper(service.service_name) + "Api"
    print(f"class {class_name}(object):")
    for op_name in service.operation_names:
        operation: OperationModel = service.operation_model(op_name)

        fn_name = camel_to_snake_case(op_name)
        input_shape = operation.input_shape.name

        if operation.output_shape:
            output_shape = operation.output_shape.name
        else:
            output_shape = "None"

        print("")
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
        print(f"    def {fn_name}(self, {param_list}) -> {output_shape}:")

        html = operation.documentation
        import pypandoc

        doc = pypandoc.convert_text(html, "rst", format="html")
        print(f'        """')
        print(f"{doc.strip()}")
        print()
        for param_name, shape in param_shapes.items():
            pdoc = pypandoc.convert_text(shape.documentation, "rst", format="html")
            pdoc = pdoc.strip().split(".")[0] + "."
            print(f":param {param_name}: {pdoc}")

        print(f'        """')
        print(f"        raise NotImplementedError")


if __name__ == "__main__":
    main()
