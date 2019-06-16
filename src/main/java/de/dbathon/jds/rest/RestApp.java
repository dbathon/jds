package de.dbathon.jds.rest;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.eclipse.microprofile.openapi.annotations.Components;
import org.eclipse.microprofile.openapi.annotations.OpenAPIDefinition;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.info.Info;
import org.eclipse.microprofile.openapi.annotations.info.License;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@ApplicationPath("/")
@OpenAPIDefinition(
    info = @Info(title = "jds - json datastore", version = "0.1", description = "description...",
        license = @License(name = "MIT", url = "https://opensource.org/licenses/MIT")),
    components = @Components(schemas = @Schema(name = "jsonObject", type = SchemaType.OBJECT)))
public class RestApp extends Application {}
