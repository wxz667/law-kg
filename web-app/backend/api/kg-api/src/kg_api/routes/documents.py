"""User document routes - CRUD operations for personal documents"""
from fastapi import APIRouter, Depends, HTTPException, status, Body
from sqlalchemy.orm import Session
from typing import List, Dict, Any
from pydantic import BaseModel
from datetime import datetime

from kg_api.database import get_db
from kg_api.models import User, UserDocument
from kg_api.schemas import UserDocumentCreate, UserDocumentUpdate, UserDocumentOut
from kg_api.routes.auth import get_current_user

router = APIRouter(prefix="/user-documents", tags=["User Documents"])


@router.get("/", response_model=List[UserDocumentOut])
def list_user_documents(
    limit: int = 50,
    offset: int = 0,
    doc_type: str = None,
    status: str = None,
    q: str = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List current user's documents with optional search"""
    query = db.query(UserDocument).filter(
        UserDocument.user_id == current_user.id)

    if doc_type:
        query = query.filter(UserDocument.doc_type == doc_type)

    if status:
        query = query.filter(UserDocument.status == status)

    if q:
        # 搜索标题和内容
        search_pattern = f"%{q}%"
        query = query.filter(
            (UserDocument.title.ilike(search_pattern)) |
            (UserDocument.content.ilike(search_pattern))
        )

    documents = query.order_by(UserDocument.created_at.desc()).offset(
        offset).limit(limit).all()
    return documents


@router.post("/", response_model=UserDocumentOut, status_code=status.HTTP_201_CREATED)
def create_user_document(
    doc_data: UserDocumentCreate = Body(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Create a new document for current user"""
    # 如果前端传入了status，使用传入的值，否则默认为draft
    initial_status = doc_data.status if hasattr(
        doc_data, 'status') and doc_data.status else "draft"

    new_doc = UserDocument(
        title=doc_data.title,
        content=doc_data.content,
        doc_type=doc_data.doc_type or "通用文书",
        user_id=current_user.id,
        status=initial_status
    )

    db.add(new_doc)
    db.commit()
    db.refresh(new_doc)

    return new_doc


@router.get("/{doc_id}", response_model=UserDocumentOut)
def get_user_document(
    doc_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get a specific document by ID"""
    doc = db.query(UserDocument).filter(
        UserDocument.id == doc_id,
        UserDocument.user_id == current_user.id
    ).first()

    if not doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Document not found"
        )

    return doc


@router.put("/{doc_id}", response_model=UserDocumentOut)
def update_user_document(
    doc_id: int,
    doc_data: UserDocumentUpdate = Body(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update a document"""
    doc = db.query(UserDocument).filter(
        UserDocument.id == doc_id,
        UserDocument.user_id == current_user.id
    ).first()

    if not doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Document not found"
        )

    # Update fields
    if doc_data.title is not None:
        doc.title = doc_data.title
    if doc_data.content is not None:
        doc.content = doc_data.content
    if doc_data.doc_type is not None:
        doc.doc_type = doc_data.doc_type
    if doc_data.status is not None:
        doc.status = doc_data.status

    db.commit()
    db.refresh(doc)

    return doc


@router.delete("/{doc_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user_document(
    doc_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Delete a document"""
    doc = db.query(UserDocument).filter(
        UserDocument.id == doc_id,
        UserDocument.user_id == current_user.id
    ).first()

    if not doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Document not found"
        )

    db.delete(doc)
    db.commit()

    return None


# 插入法条的请求模型
class InsertProvisionRequest(BaseModel):
    """插入法条请求"""
    provision_id: str  # 法条ID（Neo4j节点ID）
    mode: str  # 'cursor' 或 'append'
    law_name: str = ""  # 法律名称
    article: str = ""  # 条
    paragraph: str = ""  # 款
    item: str = ""  # 项
    content: str = ""  # 法条完整内容


@router.post("/{doc_id}/insert-provision", response_model=UserDocumentOut)
def insert_provision_to_document(
    doc_id: int,
    request: InsertProvisionRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    在文书中插入法条引用

    支持两种模式：
    1. cursor: 在光标处插入（由前端处理，后端只保存）
    2. append: 在文末附件区追加法条引用（后端处理）
    """
    # 查找文档
    doc = db.query(UserDocument).filter(
        UserDocument.id == doc_id,
        UserDocument.user_id == current_user.id
    ).first()

    if not doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Document not found"
        )

    if request.mode == 'cursor':
        # 光标处插入：由前端处理插入逻辑，后端只需保存更新的content
        # 前端会在调用此API前已经更新了content字段
        # 这里我们只需要保存即可
        if request.content:
            doc.content = request.content

        # 更新法条引用记录
        _update_provisions_record(doc, request.provision_id)

        db.commit()
        db.refresh(doc)

        return doc

    elif request.mode == 'append':
        # 文末附件模式：在文档末尾追加法条引用
        current_content = doc.content or ""

        # 检查是否已有附录部分
        appendix_marker = "附录：相关法律条文"

        if appendix_marker in current_content:
            # 已有附录，在附录前插入新的法条引用
            appendix_index = current_content.find(appendix_marker)

            # 获取附录前的内容
            before_appendix = current_content[:appendix_index].rstrip()
            appendix_content = current_content[appendix_index:]

            # 生成新的法条引用
            new_citation = _format_law_citation(
                request.law_name,
                request.article,
                request.paragraph,
                request.item
            )

            # 检查是否已经引用过该法条
            if new_citation not in appendix_content:
                # 在附录标题后插入
                appendix_lines = appendix_content.split('\n')
                new_appendix_lines = [appendix_lines[0]]  # 保留标题
                new_appendix_lines.append(new_citation)  # 添加新法条
                new_appendix_lines.extend(appendix_lines[1:])  # 保留原有内容

                new_content = before_appendix + "\n\n" + \
                    "\n".join(new_appendix_lines)
                doc.content = new_content
            # 如果已存在，不重复添加
        else:
            # 没有附录，创建新的附录部分
            new_citation = _format_law_citation(
                request.law_name,
                request.article,
                request.paragraph,
                request.item
            )

            # 清理末尾的HTML标签
            clean_content = current_content.rstrip()

            # 添加附录
            appendix_section = f"\n\n{appendix_marker}\n{new_citation}"
            doc.content = clean_content + appendix_section

        # 更新法条引用记录
        _update_provisions_record(doc, request.provision_id)

        db.commit()
        db.refresh(doc)

        return doc

    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid mode: {request.mode}. Must be 'cursor' or 'append'"
        )


def _format_law_citation(law_name: str, article: str,
                         paragraph: str = "", item: str = "") -> str:
    """
    格式化法条引用为刑事判决书标准格式
    例如：《中华人民共和国刑法》第二百二十五条第（一）项
    """
    if not law_name and not article:
        return ""

    result = f"《{law_name}》" if law_name else ""
    result += article

    if paragraph:
        result += paragraph
    if item:
        result += item

    return result


def _update_provisions_record(doc: UserDocument, provision_id: str):
    """
    更新文档的法条引用记录
    provisions字段存储JSON格式的法条ID列表
    """
    import json

    current_provisions = []
    if doc.provisions:
        try:
            current_provisions = json.loads(doc.provisions)
        except:
            current_provisions = []

    # 添加新的法条ID（去重）
    if provision_id not in current_provisions:
        current_provisions.append(provision_id)
        doc.provisions = json.dumps(current_provisions)
