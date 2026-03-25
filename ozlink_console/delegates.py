from __future__ import annotations

from PySide6.QtCore import Qt, QRect
from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import QStyledItemDelegate, QStyle

class SourceRelationDelegate(QStyledItemDelegate):
    def paint(self, painter: QPainter, option, index) -> None:
        base = index.data(Qt.UserRole + 10) or index.data(Qt.DisplayRole) or ""
        relation = index.data(Qt.UserRole + 11) or ""
        overlay_state = index.data(Qt.UserRole + 12) or ""

        painter.save()
        option.widget.style().drawPrimitive(QStyle.PE_PanelItemViewItem, option, painter, option.widget)

        text_rect: QRect = option.rect.adjusted(4, 0, -4, 0)
        if option.state & QStyle.State_Selected:
            painter.fillRect(option.rect, option.palette.highlight())
            base_color = option.palette.highlightedText().color()
        else:
            base_color = QColor("white")

        base_font = option.font
        if overlay_state == "Allocated":
            base_font.setBold(True)

        painter.setFont(base_font)
        painter.setPen(QPen(base_color))
        fm = painter.fontMetrics()
        painter.drawText(text_rect, Qt.AlignVCenter | Qt.TextSingleLine, base)
        base_width = fm.horizontalAdvance(base)

        if relation:
            rel_rect = QRect(text_rect.left() + base_width + 6, text_rect.top(), max(0, text_rect.width() - base_width - 6), text_rect.height())
            if overlay_state == "Allocated":
                rel_color = QColor("#00D8FF")
            else:
                rel_color = QColor("#8cd2ff")
            if option.state & QStyle.State_Selected:
                rel_color = option.palette.highlightedText().color()
            painter.setFont(option.font)
            painter.setPen(QPen(rel_color))
            painter.drawText(rel_rect, Qt.AlignVCenter | Qt.TextSingleLine, relation)

        painter.restore()
