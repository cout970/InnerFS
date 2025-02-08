use crate::metadata_db::{FileRow, FILE_KIND_DIRECTORY, ROOT_DIRECTORY_ID};
use crate::inner_file_system::InnerFileSystem;
use crate::utils::{format_timestamp, humanize_bytes_binary};
use crate::AnyError;
use anyhow::Context;
use log::{error, info};
use std::io::{BufReader, Cursor, Read};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;
use tiny_http::{Header, Request, Response};
use url::Url;
use xml_dom::level2::convert::as_element;
use xml_dom::level2::{get_implementation, Document, Element, Node, RefNode};

pub fn handle_request(mut request: Request, fs: &mut InnerFileSystem) -> Result<(), AnyError> {
    let start = Instant::now();
    let method = request.method().as_str();
    info!("IN : {} {}", method, request.url());

    if method == "GET" && request.url() == "/" {
        let home = Response::from_string(include_str!("./web/dist/index.html"))
            .with_status_code(200)
            .with_header_str("Content-Type: text/html");
        request.respond(home)?;
        return Ok(());
    }

    let result = match method {
        "GET" => handle_get(&mut request, fs),
        "HEAD" => handle_head(&mut request, fs),
        "OPTIONS" => handle_options(&mut request, fs),
        "PUT" => handle_put(&mut request, fs),
        "DELETE" => handle_delete(&mut request, fs),
        "PROPFIND" => handle_propfind(&mut request, fs),
        "PROPPATCH" => handle_proppatch(&mut request, fs),
        "COPY" => handle_copy(&mut request, fs),
        "MOVE" => handle_move(&mut request, fs),
        "LOCK" => handle_lock(&mut request, fs),
        "UNLOCK" => handle_unlock(&mut request, fs),
        "MKCOL" => handle_mkcol(&mut request, fs),
        _ => Ok(Response::from_string("Not Found").with_status_code(404)),
    };

    let response = result.unwrap_or_else(|e| {
        error!("ERR: {}", e);
        Response::from_string(e.to_string()).with_status_code(500)
    });

    let elapsed = start.elapsed().as_millis();
    info!(
        "OUT: {} ({}, {} ms)",
        response.status_code().0,
        humanize_bytes_binary(response.data_length().unwrap_or(0)),
        elapsed
    );

    let response = response
        .with_header_str("Access-Control-Allow-Origin: *")
        .with_header_str(
            "Access-Control-Allow-Methods: OPTIONS, GET, HEAD, POST, PUT, DELETE, PROPFIND, PROPPATCH, MKCOL",
        )
        .with_header_str("Access-Control-Allow-Headers: Content-Type, Depth, Destination, Overwrite, Authorization");

    request.respond(response)?;
    Ok(())
}

/// Provides valid HTTP methods for WebDAV and supported DAV versions
pub fn handle_options(_request: &mut Request, _fs: &mut InnerFileSystem) -> Result<Response<Cursor<Vec<u8>>>, AnyError> {
    Ok(Response::from_string("")
        .with_header_str("Allow: OPTIONS, GET, HEAD, POST, PUT, DELETE, PROPFIND, PROPPATCH, MKCOL")
        .with_header_str("DAV: 1, 2"))
}

/// Get the content of a file
pub fn handle_get(request: &mut Request, fs: &mut InnerFileSystem) -> Result<Response<Cursor<Vec<u8>>>, AnyError> {
    let file: Option<FileRow> = fs.get_file_by_path(&strip_path(request.url()))?;

    if let Some(file) = file {
        let content = fs.read_all(file.id)?;
        let mut response = Response::from_data(content);
        let headers = [
            Header::from_str("Content-Type: application/octet-stream").unwrap(),
            Header::from_str("Content-Disposition: inline").unwrap(),
            Header::from_str(&format!("Content-Length: {}", file.size)).unwrap(),
            Header::from_str(&format!("ETag: \"{}\"", file.sha512)).unwrap(),
            Header::from_str(&format!("Last-Modified: {}", format_timestamp(file.updated_at))).unwrap(),
        ];

        for h in headers {
            response.add_header(h);
        }

        Ok(response)
    } else {
        Ok(Response::from_string("Not Found").with_status_code(404))
    }
}

/// Get the metadata of a file
pub fn handle_head(request: &mut Request, fs: &mut InnerFileSystem) -> Result<Response<Cursor<Vec<u8>>>, AnyError> {
    let file: Option<FileRow> = fs.get_file_by_path(&strip_path(request.url()))?;

    if let Some(file) = file {
        let mut response = Response::from_data(vec![]);
        let headers = [
            Header::from_str("Content-Type: application/octet-stream").unwrap(),
            Header::from_str(&format!("Content-Length: {}", file.size)).unwrap(),
            Header::from_str(&format!("ETag: \"{}\"", file.sha512)).unwrap(),
            Header::from_str(&format!("Last-Modified: {}", format_timestamp(file.updated_at))).unwrap(),
        ];

        for h in headers {
            response.add_header(h);
        }

        Ok(response)
    } else {
        Ok(Response::from_string("Not Found").with_status_code(404))
    }
}

/// Update the content of a file
pub fn handle_put(request: &mut Request, fs: &mut InnerFileSystem) -> Result<Response<Cursor<Vec<u8>>>, AnyError> {
    let path = strip_path(request.url());
    let mut file = fs.get_file_by_path(&path)?;

    if file.is_none() {
        let parent_path = dirname(&path);
        let filename = basename(&path);

        let parent_id = fs.get_file_id_by_path(&parent_path)?;
        if parent_id.is_none() {
            return Ok(Response::from_string("Missing parent directory").with_status_code(400));
        }

        file = Some(fs.mknod(parent_id.unwrap(), &filename, 0, 0, libc::S_IFREG + 0o644)?);
    }
    let file = file.unwrap();

    let mut content = vec![];
    request.as_reader().read_to_end(&mut content)?;

    fs.write_all(file.id, &content)?;

    Ok(Response::from_string("OK").with_status_code(200))
}

/// Delete a file or empty folder
pub fn handle_delete(request: &mut Request, fs: &mut InnerFileSystem) -> Result<Response<Cursor<Vec<u8>>>, AnyError> {
    let path = strip_path(request.url());
    let file = fs.get_file_by_path(&path)?;

    let Some(file) = file else {
        return Ok(Response::from_string("Not Found").with_status_code(404));
    };

    let parent_id = fs.get_file_parent_id(file.id)?;

    let Some(parent_id) = parent_id else {
        return Ok(Response::from_string("Not Found").with_status_code(404));
    };

    fs.unlink(parent_id, &file.name)?;

    Ok(Response::from_string("No Content").with_status_code(204))
}

/// Copy a file to another location
pub fn handle_copy(request: &mut Request, fs: &mut InnerFileSystem) -> Result<Response<Cursor<Vec<u8>>>, AnyError> {
    let path = strip_path(request.url());
    let parent_path = dirname(&path);
    let filename = basename(&path);

    let override_flag = request.headers().iter().find(|h| h.field.equiv("Overwrite"));
    let override_flag = override_flag.map(|h| h.value.as_str()).unwrap_or("T") == "T";

    let dest = request.headers().iter().find(|h| h.field.equiv("Destination"));
    let Some(dest) = dest else {
        return Ok(Response::from_string("Missing Destination header").with_status_code(400));
    };
    let dest = strip_path(dest.value.as_str()).to_string();

    if !override_flag && fs.get_file_id_by_path(&dest)?.is_some() {
        return Ok(Response::from_string("Destination already exists").with_status_code(412));
    }

    let parent_id = fs.get_file_id_by_path(&parent_path)?;
    let Some(parent_id) = parent_id else {
        return Ok(Response::from_string("Missing parent directory").with_status_code(400));
    };

    let new_parent_path = dirname(&dest);
    let new_filename = basename(&dest);

    let new_parent_id = fs.get_file_id_by_path(&new_parent_path)?;
    let Some(new_parent_id) = new_parent_id else {
        return Ok(Response::from_string("Missing new parent directory").with_status_code(400));
    };

    fs.copy_file(parent_id, &filename, new_parent_id, &new_filename)?;

    Ok(Response::from_string("Created").with_status_code(201))
}

/// Move a file to another location
pub fn handle_move(request: &mut Request, fs: &mut InnerFileSystem) -> Result<Response<Cursor<Vec<u8>>>, AnyError> {
    let path = strip_path(request.url());
    let parent_path = dirname(&path);
    let filename = basename(&path);

    let override_flag = request.headers().iter().find(|h| h.field.equiv("Overwrite"));
    let override_flag = override_flag.map(|h| h.value.as_str()).unwrap_or("T") == "T";

    let dest = request.headers().iter().find(|h| h.field.equiv("Destination"));
    let Some(dest) = dest else {
        return Ok(Response::from_string("Missing Destination header").with_status_code(400));
    };
    let dest = strip_path(dest.value.as_str()).to_string();

    if !override_flag && fs.get_file_id_by_path(&dest)?.is_some() {
        return Ok(Response::from_string("Destination already exists").with_status_code(412));
    }

    let parent_id = fs.get_file_id_by_path(&parent_path)?;
    let Some(parent_id) = parent_id else {
        return Ok(Response::from_string("Missing parent directory").with_status_code(400));
    };

    let new_parent_path = dirname(&dest);
    let new_filename = basename(&dest);

    let new_parent_id = fs.get_file_id_by_path(&new_parent_path)?;
    let Some(new_parent_id) = new_parent_id else {
        return Ok(Response::from_string("Missing new parent directory").with_status_code(400));
    };

    fs.move_file(parent_id, &filename, new_parent_id, &new_filename)?;

    Ok(Response::from_string("Created").with_status_code(201))
}

/// Lock a resource, not implemented
pub fn handle_lock(_request: &mut Request, _fs: &mut InnerFileSystem) -> Result<Response<Cursor<Vec<u8>>>, AnyError> {
    Ok(Response::from_string("Not Implemented").with_status_code(501))
}

/// Unlock a resource, not implemented
pub fn handle_unlock(_request: &mut Request, _fs: &mut InnerFileSystem) -> Result<Response<Cursor<Vec<u8>>>, AnyError> {
    Ok(Response::from_string("Not Implemented").with_status_code(501))
}

/// Creates a new directory
pub fn handle_mkcol(request: &mut Request, fs: &mut InnerFileSystem) -> Result<Response<Cursor<Vec<u8>>>, AnyError> {
    let path = strip_path(request.url());
    let parent_path = dirname(&path);
    let filename = basename(&path);

    let parent_id = fs.get_file_id_by_path(&parent_path)?;
    let Some(parent_id) = parent_id else {
        return Ok(Response::from_string("Missing parent directory").with_status_code(400));
    };

    fs.mkdir(parent_id, &filename, 0, 0, libc::S_IFDIR + 0o755)?;

    Ok(Response::from_string("Created").with_status_code(201))
}

#[derive(Clone)]
struct FileWithPath {
    path: String,
    local_name: String,
    file: FileRow,
}

/// Get properties of a file or directory
pub fn handle_propfind(request: &mut Request, fs: &mut InnerFileSystem) -> Result<Response<Cursor<Vec<u8>>>, AnyError> {
    let depth = request
        .headers()
        .iter()
        .find(|h| h.field.equiv("Depth"))
        .map(|h| h.value.as_str())
        .unwrap_or("0")
        .parse::<i32>()
        .context("Invalid Depth header")?;

    let requested_properties = parse_propfind_body(request)?;
    let path = strip_path(request.url());

    let file: Option<FileRow> = if path.is_empty() || &path == "/" {
        Some(fs.get_file_or_err(ROOT_DIRECTORY_ID)?)
    } else {
        fs.get_file_by_path(&path)?
    };

    let Some(file) = file else {
        return Ok(Response::from_string("Not Found").with_status_code(404));
    };

    fn get_files(
        fs: &mut InnerFileSystem,
        file: FileRow,
        path: &str,
        depth: i32,
        files: &mut Vec<FileWithPath>,
    ) -> Result<(), AnyError> {
        files.push(FileWithPath {
            path: path.to_owned(),
            local_name: file.name.clone(),
            file: file.clone(),
        });

        if file.kind == FILE_KIND_DIRECTORY && depth > 0 {
            for entry in fs.get_directory_entries(file.id)? {
                if entry.name == "." || entry.name == ".." {
                    continue;
                }
                let sub_file = fs.get_file_or_err(entry.entry_file_id)?;
                let entry_path = format!("{}{}", path, entry.name);

                files.push(FileWithPath {
                    path: entry_path.clone(),
                    local_name: entry.name.clone(),
                    file: sub_file.clone(),
                });

                if sub_file.kind == FILE_KIND_DIRECTORY && depth > 1 {
                    get_files(fs, sub_file, &format!("{}/", entry_path), depth - 1, files)?;
                }
            }
        }

        Ok(())
    }

    let mut files = vec![];
    let root = if !path.ends_with("/") {
        format!("{}/", path)
    } else {
        path.clone()
    };
    get_files(fs, file, &root, depth, &mut files)?;

    let response = generate_propfind_response(&files, requested_properties)?;

    Ok(Response::from_string(response)
        .with_status_code(207)
        .with_header_str("Content-Type: text/xml"))
}

pub fn handle_proppatch(
    _request: &mut Request,
    _fs: &mut InnerFileSystem,
) -> Result<Response<Cursor<Vec<u8>>>, AnyError> {
    Ok(Response::from_string("Not Implemented").with_status_code(501))
}

fn parse_propfind_body(request: &mut Request) -> Result<Vec<String>, AnyError> {
    let body = xml_dom::parser::read_reader(BufReader::new(request.as_reader()))?;
    let root = body
        .document_element()
        .context("Missing XML root element, maybe the request is empty?")?;

    let propfind = as_element(&root)?;
    let prop = propfind.first_child().context("Missing <prop> element")?;

    let mut requested_properties = vec![];

    if prop.node_name().local_name() == "allprop" {
        requested_properties.push("resourcetype".to_owned());
        requested_properties.push("getcontenttype".to_owned());
        requested_properties.push("getcontentlength".to_owned());
        requested_properties.push("displayname".to_owned());
        requested_properties.push("getlastmodified".to_owned());
        requested_properties.push("getetag".to_owned());
        requested_properties.push("creationdate".to_owned());
        requested_properties.push("quota-available-bytes".to_owned());
        requested_properties.push("quota-used-bytes".to_owned());
        requested_properties.push("getcontentlanguage".to_owned());
    } else {
        for node in prop.child_nodes() {
            requested_properties.push(node.node_name().local_name().to_owned());
        }
    }

    Ok(requested_properties)
}

fn generate_propfind_response(files: &[FileWithPath], requested_properties: Vec<String>) -> Result<String, AnyError> {
    let mut doc = get_implementation().create_document(None, None, None)?;

    // Create the root <D:multistatus> element
    let mut multistatus = doc.create_element("D:multistatus")?;
    multistatus.set_attribute("xmlns:D", "DAV:")?;
    doc.append_child(multistatus.clone())?;

    for FileWithPath { path, local_name, file } in files {
        // Create <D:response> element
        let mut response = doc.create_element("D:response")?;
        multistatus.append_child(response.clone())?;

        // Add <D:href>
        let mut href_element = doc.create_element("D:href")?;
        href_element.append_child(doc.create_text_node(&format!("/{}", path.trim_start_matches("/"))))?;
        response.append_child(href_element)?;

        // Add <D:propstat>
        let mut propstat = doc.create_element("D:propstat")?;
        response.append_child(propstat.clone())?;

        // Add <D:prop>
        let mut prop = doc.create_element("D:prop")?;
        propstat.append_child(prop.clone())?;

        if requested_properties.contains(&"resourcetype".to_owned()) {
            let mut resourcetype = doc.create_element("D:resourcetype")?;
            if file.kind == FILE_KIND_DIRECTORY {
                let collection = doc.create_element("D:collection")?;
                resourcetype.append_child(collection)?;
            }
            prop.append_child(resourcetype)?;
        }

        if file.kind != FILE_KIND_DIRECTORY && requested_properties.contains(&"getcontenttype".to_owned()) {
            let mime = mime_guess::from_path(&file.name)
                .first()
                .map(|i| i.to_string())
                .unwrap_or_else(|| "application/octet-stream".to_owned());
            add_property(&doc, &mut prop, "D:getcontenttype", &mime)?;
        }
        if requested_properties.contains(&"getcontentlength".to_owned()) {
            add_property(&doc, &mut prop, "D:getcontentlength", &file.size.to_string())?;
        }
        if requested_properties.contains(&"displayname".to_owned()) {
            add_property(&doc, &mut prop, "D:displayname", local_name)?;
        }
        if requested_properties.contains(&"getlastmodified".to_owned()) {
            add_property(&doc, &mut prop, "D:getlastmodified", &format_timestamp(file.updated_at))?;
        }
        if requested_properties.contains(&"getetag".to_owned()) {
            add_property(&doc, &mut prop, "D:getetag", &file.sha512)?;
        }
        if requested_properties.contains(&"creationdate".to_owned()) {
            add_property(&doc, &mut prop, "D:creationdate", &format_timestamp(file.created_at))?;
        }
        if requested_properties.contains(&"quota-available-bytes".to_owned()) {
            add_property(&doc, &mut prop, "D:quota-available-bytes", "1000000000")?;
        }
        if requested_properties.contains(&"quota-used-bytes".to_owned()) {
            add_property(&doc, &mut prop, "D:quota-used-bytes", "0")?;
        }
        if requested_properties.contains(&"getcontentlanguage".to_owned()) {
            add_property(&doc, &mut prop, "D:getcontentlanguage", "en")?;
        }

        let mut status = doc.create_element("D:status")?;
        status.append_child(doc.create_text_node("HTTP/1.1 200 OK"))?;
        propstat.append_child(status.clone())?;
    }

    Ok(format!("{:#}", doc))
}

fn add_property(doc: &RefNode, node: &mut RefNode, name: &str, value: &str) -> Result<(), AnyError> {
    let mut element = doc.create_element(name)?;
    element.append_child(doc.create_text_node(value))?;
    node.append_child(element)?;
    Ok(())
}

fn dirname(path: &str) -> String {
    let path = PathBuf::from(path);

    path.parent()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| "/".to_owned())
}

fn basename(path: &str) -> String {
    PathBuf::from(path)
        .file_name()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| "".to_owned())
}

fn strip_path(url: &str) -> String {
    let base = Url::parse("file:///").unwrap();
    let path = base.join(url).expect("Invalid URL").path().to_owned();
    let path = percent_encoding::percent_decode(path.as_bytes()).decode_utf8_lossy();
    let path = path.trim_start_matches('/');
    let path = path.trim_end_matches('/');
    let path = if path.is_empty() { "/" } else { path };
    path.to_owned()
}

trait ResponseExt {
    fn with_header_str(self, value: &str) -> Self;
}

impl<R> ResponseExt for Response<R>
where
    R: Read,
{
    fn with_header_str(mut self, value: &str) -> Self {
        self.add_header(Header::from_str(value).unwrap());
        self
    }
}
