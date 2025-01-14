from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.units import inch
import io
from datetime import datetime
from typing import List, Dict, Any

class PDFReportGenerator:
    def __init__(self):
        self.styles = getSampleStyleSheet()
        self.title_style = ParagraphStyle(
            'CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=24,
            spaceAfter=30
        )
        self.heading_style = ParagraphStyle(
            'CustomHeading',
            parent=self.styles['Heading2'],
            fontSize=16,
            spaceAfter=12
        )
        self.body_style = self.styles['Normal']

    def generate_report(
        self,
        query_patterns: List[Dict[str, Any]],
        suggestions: List[Dict[str, Any]]
    ) -> bytes:
        """Generate a PDF report with query analysis and optimization suggestions"""
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=letter,
            rightMargin=72,
            leftMargin=72,
            topMargin=72,
            bottomMargin=72
        )

        # Build the document content
        story = []

        # Title
        story.append(Paragraph(
            "QuerySight Performance Analysis Report",
            self.title_style
        ))
        story.append(Paragraph(
            f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            self.body_style
        ))
        story.append(Spacer(1, 20))

        # Query Patterns Section
        story.append(Paragraph("Query Pattern Analysis", self.heading_style))
        story.extend(self._create_query_patterns_section(query_patterns))
        story.append(Spacer(1, 20))

        # Optimization Suggestions Section
        story.append(Paragraph("Optimization Suggestions", self.heading_style))
        story.extend(self._create_suggestions_section(suggestions))

        # Build the PDF
        doc.build(story)
        pdf_bytes = buffer.getvalue()
        buffer.close()
        return pdf_bytes

    def _create_query_patterns_section(self, patterns: List[Dict[str, Any]]) -> List:
        """Create the query patterns section of the report"""
        elements = []
        
        # Top Patterns Table
        if patterns:
            table_data = [['Query Pattern', 'Frequency', 'Avg Duration (ms)', 'Avg Rows']]
            for pattern in patterns[:5]:  # Show top 5 patterns
                table_data.append([
                    pattern['pattern'],
                    str(pattern['frequency']),
                    f"{pattern['avg_duration_ms']:.2f}",
                    str(pattern['avg_read_rows'])
                ])

            table = Table(table_data, colWidths=[4*inch, 1*inch, 1.5*inch, 1*inch])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 14),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 12),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            elements.append(table)

        return elements

    def _create_suggestions_section(self, suggestions: List[Dict[str, Any]]) -> List:
        """Create the optimization suggestions section of the report"""
        elements = []

        for idx, suggestion in enumerate(suggestions, 1):
            # Suggestion Title
            elements.append(Paragraph(
                f"{idx}. {suggestion['title']} (Impact: {suggestion['impact_level']})",
                self.styles['Heading3']
            ))
            elements.append(Spacer(1, 10))

            # Problem Description
            if 'problem_description' in suggestion:
                elements.append(Paragraph(
                    f"Problem: {suggestion['problem_description']}",
                    self.body_style
                ))
                elements.append(Spacer(1, 10))

            # Benefits and Risks
            if 'optimization_details' in suggestion:
                details = suggestion['optimization_details']
                if 'benefits' in details:
                    benefits_text = "Benefits:<br/>" + "<br/>".join(
                        f"• {benefit}" for benefit in details['benefits']
                    )
                    elements.append(Paragraph(benefits_text, self.body_style))
                    elements.append(Spacer(1, 10))

                if 'potential_risks' in details:
                    risks_text = "Considerations:<br/>" + "<br/>".join(
                        f"• {risk}" for risk in details['potential_risks']
                    )
                    elements.append(Paragraph(risks_text, self.body_style))
                    elements.append(Spacer(1, 10))

            # Implementation Steps
            if 'implementation_steps' in suggestion:
                steps_text = "Implementation Steps:<br/>" + "<br/>".join(
                    f"{i+1}. {step}" for i, step in enumerate(suggestion['implementation_steps'])
                )
                elements.append(Paragraph(steps_text, self.body_style))
            
            elements.append(Spacer(1, 20))

        return elements
